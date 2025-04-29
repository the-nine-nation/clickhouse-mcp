#!/usr/bin/env python3
import asyncio
import os
import sys
import logging
import re
from pprint import pprint

# 设置日志级别
logging.basicConfig(level=logging.INFO)

# 设置环境变量 - 必须在任何导入之前,请用这个脚本测试
os.environ["CLICKHOUSE_ENABLED"] = "true"
os.environ["CLICKHOUSE_HOST"] = "10.10.25.32"
os.environ["CLICKHOUSE_PORT"] = "9000"
os.environ["CLICKHOUSE_HTTP_PORT"] = "8123"
os.environ["CLICKHOUSE_DATABASE"] = "***"
os.environ["CLICKHOUSE_USERNAME"] = "default"
os.environ["CLICKHOUSE_PASSWORD"] = "***"
os.environ["MAX_ROWS"] = "10"

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入必要的模块
from clickhouse_mcp.lifespan_code import app_lifespan, DatabaseConnection, app_context, DB_CONFIG
from main import mcp, clickhouse_execute_read, get_connection

# 专用于调试的函数
def inspect_connection(title=""):
    """详细检查连接对象"""
    print(f"\n----- {title} -----")
    print(f"app_context.connection: {app_context.connection}")
    print(f"app_context.connection_mode: {app_context.connection_mode}")
    
    if app_context.connection:
        print(f"连接类型: {type(app_context.connection)}")
        print(f"连接DB: {app_context.connection.database}")
        print(f"连接类型: {app_context.connection.connection_type}")
        conn_obj = app_context.connection.connection
        print(f"实际连接对象: {conn_obj}")
        print(f"实际连接对象类型: {type(conn_obj)}")
        
        # 检查get_connection函数
        test_conn = get_connection()
        print(f"get_connection()返回值: {test_conn}")
        print(f"app_context.connection == test_conn: {app_context.connection == test_conn}")


async def test_clickhouse_mcp():
    """测试 ClickHouse MCP 服务器"""
    try:
        # 手动调用 lifespan 函数来初始化连接
        print("初始化数据库连接...")
        
        async with app_lifespan(mcp):
            print(f"连接状态: {app_context.connection is not None}")
            print(f"连接模式: {app_context.connection_mode}")
            
            if not app_context.connection:
                print("警告: 连接未建立，请检查连接配置和lifespan代码")
                return
                
            print(f"数据库: {app_context.connection.database}")
            
            # 基础查询测试
            basic_queries = [
                "SELECT 1",
                "SHOW DATABASES",
                "SHOW TABLES FROM ueba LIMIT 5",
                "SELECT version()",
                "SELECT * FROM system.one LIMIT 1",
                "SELECT * FROM system.databases LIMIT 3",
            ]
            
            print("\n=== 基础查询测试 ===")
            for query in basic_queries:
                print(f"\n--- 执行: {query} ---")
                try:
                    result = await clickhouse_execute_read(
                        sql_query=query,
                        max_rows=int(os.environ.get("MAX_ROWS", 10))
                    )
                    print(result)
                except Exception as e:
                    print(f"执行查询出错: {e}")
            
            # 测试特殊表名处理 - 获取一个真实表
            print("\n=== 特殊表名测试 ===")
            try:
                tables_result = await clickhouse_execute_read(
                    sql_query="SELECT name FROM system.tables WHERE database = 'ueba' LIMIT 5",
                    max_rows=5
                )
                
                # 从结果中提取表名
                if "name" in tables_result:
                    table_names = []
                    lines = tables_result.split('\n')
                    # 跳过标题行
                    for i in range(1, len(lines)):
                        if lines[i].strip():
                            table_names.append(lines[i].strip())
                    
                    # 测试每个表
                    for table_name in table_names:
                        if '.' in table_name:  # 只测试包含点的特殊表名
                            print(f"\n--- 测试特殊表名: {table_name} ---")
                            # 测试不带引号的直接查询
                            try:
                                describe_query = f"DESCRIBE ueba.{table_name}"
                                print(f"查询: {describe_query}")
                                result = await clickhouse_execute_read(
                                    sql_query=describe_query,
                                    max_rows=3
                                )
                                print(f"结果:\n{result}")
                            except Exception as e:
                                print(f"直接查询出错: {e}")
                            
                            # 测试带引号的查询
                            try:
                                describe_query = f"DESCRIBE ueba.`{table_name}`"
                                print(f"带引号查询: {describe_query}")
                                result = await clickhouse_execute_read(
                                    sql_query=describe_query,
                                    max_rows=3
                                )
                                print(f"结果:\n{result}")
                            except Exception as e:
                                print(f"引号查询出错: {e}")
                            
                            # 只测试一个表即可
                            break
            except Exception as e:
                print(f"获取表名出错: {e}")
        
        print("\n测试完成")
        
    except ImportError as e:
        print(f"导入MCP服务器失败: {e}")
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        print(traceback.format_exc())

if __name__ == "__main__":
    asyncio.run(test_clickhouse_mcp()) 