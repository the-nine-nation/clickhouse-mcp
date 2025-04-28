from fastmcp import FastMCP
from typing import Annotated, Dict, Any, Optional
from pydantic import Field
from typing import Any, Optional
import logging
import traceback
import os


from lifespan_code import app_lifespan, DatabaseConnection,app_context,CLICKHOUSE_NATIVE_AVAILABLE,CLICKHOUSE_HTTP_AVAILABLE,DB_CONFIG
from func import execute_http_query,process_native_result,format_query_results

# Import ClickHouseClient directly here
try:
    from clickhouse_driver import Client as ClickHouseClient
except ImportError:
    ClickHouseClient = None

mcp = FastMCP("My MCP Server", lifespan=app_lifespan)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 从文件加载资源描述
def load_resource_description(file_path=""):
    """从文件加载资源工具描述，文件路径必须指定,如果没有则为空"""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            return content
        except Exception as e:
            logger.error(f"Error reading resource description file {file_path}: {e}")
            return ""
    else:
        return ""

@mcp.tool(
    name="clickhouse_execute_read",
    description="Execute read-only ClickHouse SQL code. Only SELECT, SHOW, DESCRIBE, EXPLAIN allowed; queries are validated before execution.\n\n"+load_resource_description(os.environ.get("CLICKHOUSE_RESOURCE_DESC_FILE", "")),
)
async def clickhouse_execute_read(sql_query: Annotated[str, Field(description="The SQL code to execute (only SELECT, SHOW, DESCRIBE, EXPLAIN allowed)")],
                                 max_rows: Annotated[int, Field(description="Maximum number of rows to return", default=10, le=100)],
                                 params: Annotated[Optional[Dict[str, Any]], Field(description="Parameters for the SQL query (for parameterized queries)", default=None)] = None) -> str:
    """
    Execute read-only ClickHouse SQL code. Only SELECT, SHOW, DESCRIBE, EXPLAIN allowed; queries are validated before execution.
    """
    try:          
        # 执行查询
        return await execute_db_query(sql_query, params or {}, max_rows)
        
    except KeyError as e:
        return f"Error: Missing required parameter: {e.args[0]}"
    except Exception as e:
        return f"Error: {str(e)}\n{traceback.format_exc()}"
    


def get_connection() -> Optional[DatabaseConnection]:
    """获取由生命周期管理的ClickHouse数据库连接"""
    if not app_context.connection:
        logger.error("ClickHouse connection not available or not enabled.")
        return None
    return app_context.connection


async def execute_db_query(query: str, params: dict, max_rows: int) -> str:
    """执行数据库查询并返回结果"""
    try:
        # 安全检查：只允许SELECT、SHOW、DESCRIBE、EXPLAIN等只读操作
        query_lower = query.strip().lower()
        allowed_prefixes = ("select", "show", "describe", "desc", "explain")
        if not query_lower.startswith(allowed_prefixes):
            return f"Error: Only read operations (SELECT, SHOW, DESCRIBE, EXPLAIN) are allowed. Rejected query: {query}"
        
        # 防止多语句执行
        if ";" in query[:-1]:  # 允许查询末尾有分号
            return f"Error: Multiple statements are not allowed. Rejected query: {query}"
        
        # 获取连接
        conn = get_connection()
        if not conn:
            return f"Error: Could not connect to ClickHouse database"
        
        result = None
        connection_error = None
        
        # 1. 首先使用启动时确定的连接方式
        try:
            # 使用已确定的连接方式执行查询
            if app_context.connection_mode == "http":
                # 使用HTTP接口
                result = execute_http_query(
                    DB_CONFIG["host"], 
                    DB_CONFIG["http_port"], 
                    conn.database, 
                    query, 
                    DB_CONFIG["username"], 
                    DB_CONFIG["password"],
                    params,  
                    max_rows
                )
                if not result.get("success", False):
                    raise Exception(result.get("error", "Unknown HTTP query error"))
            else:
                # 使用原生客户端
                result_set = conn.connection.execute(query, params)
                result = process_native_result(result_set, query_lower, max_rows)
        except Exception as e:
            # 记录错误，准备尝试另一种方式
            connection_error = str(e)
            logger.warning(f"Query using {app_context.connection_mode} mode failed: {e}")
            result = None
        
        # 2. 如果主要连接方式失败，尝试另一种方式
        if result is None:
            try:
                alternate_mode = "http" if app_context.connection_mode == "native" else "native"
                logger.info(f"Trying alternate connection mode: {alternate_mode}")
                
                if alternate_mode == "http" and CLICKHOUSE_HTTP_AVAILABLE:
                    # 尝试HTTP连接
                    result = execute_http_query(
                        DB_CONFIG["host"], 
                        DB_CONFIG["http_port"], 
                        conn.database, 
                        query, 
                        DB_CONFIG["username"], 
                        DB_CONFIG["password"],
                        params, 
                        max_rows
                    )
                    if not result.get("success", False):
                        raise Exception(result.get("error", "Unknown HTTP query error"))
                    
                elif alternate_mode == "native" and CLICKHOUSE_NATIVE_AVAILABLE and ClickHouseClient is not None:
                    # 临时创建原生客户端
                    try:
                        client = ClickHouseClient(
                            host=DB_CONFIG["host"],
                            port=DB_CONFIG["port"],
                            database=DB_CONFIG["database"],
                            user=DB_CONFIG["username"],
                            password=DB_CONFIG["password"],
                            settings={'readonly': 1}
                        )
                        result_set = client.execute(query, params)
                        result = process_native_result(result_set, query_lower, max_rows)
                        
                        # 关闭临时连接
                        if hasattr(client, 'disconnect'):
                            client.disconnect()
                    except Exception as native_error:
                        raise Exception(f"Native connection failed: {native_error}")
                else:
                    raise Exception(f"Alternate connection mode {alternate_mode} not available")
                
                # 如果替代方式成功，考虑切换连接模式
                logger.info(f"Query succeeded using alternate mode {alternate_mode}")
                
            except Exception as alt_error:
                # 两种方式都失败了，返回综合错误信息
                error_msg = f"Primary connection ({app_context.connection_mode}) error: {connection_error}\n"
                error_msg += f"Alternate connection error: {alt_error}"
                return f"Database error: {error_msg}"
        
        # 格式化结果为字符串
        if result:
            return format_query_results(result)
        else:
            return "Query executed but no results were returned."
    
    except Exception as e:
        return f"Database error: {str(e)}\n{traceback.format_exc()}"

# 添加主函数入口点
if __name__ == "__main__":
    # 直接运行服务器
    mcp.run()
    