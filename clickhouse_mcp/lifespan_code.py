from typing import AsyncIterator
from pydantic import Field, BaseModel
from typing import Any, Optional
import logging
import time

import os
import traceback
from contextlib import asynccontextmanager
import requests

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "enabled": os.environ.get("CLICKHOUSE_ENABLED", "true"),
    "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.environ.get("CLICKHOUSE_PORT", "9000")),
    "http_port": int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),  # 使用CLICKHOUSE_HTTP_PORT作为HTTP端口
    "database": os.environ.get("CLICKHOUSE_DATABASE", "default"),
    "username": os.environ.get("CLICKHOUSE_USERNAME", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    "max_rows": int(os.environ.get("MAX_ROWS", "10")),  # 默认每次查询最多返回50行
}

# 修复Pydantic模型，不使用Field(default_factory=time)
class DatabaseConnection(BaseModel):
    connection: Any  # The actual database connection object
    database: str
    connection_type: str  # "native" or "http"
    last_used: float = Field(default_factory=lambda: time.time())
    
    model_config = {
        "arbitrary_types_allowed": True
    }
    
class AppContext(BaseModel):
    connection: Optional[DatabaseConnection] = None
    connection_mode: Optional[str] = None
    connection_ttl: int = 3600  # 1 hour
    
    model_config = {
        "arbitrary_types_allowed": True
    }

app_context = AppContext()


# Import database drivers
try:
    from clickhouse_driver import Client as ClickHouseClient
    CLICKHOUSE_NATIVE_AVAILABLE = True
    logger.info("ClickHouse native driver available")
except ImportError:
    ClickHouseClient = None
    CLICKHOUSE_NATIVE_AVAILABLE = False
    logger.warning("ClickHouse native driver not available")

try:
    import requests
    CLICKHOUSE_HTTP_AVAILABLE = True
    logger.info("ClickHouse HTTP client available")
except ImportError:
    CLICKHOUSE_HTTP_AVAILABLE = False
    logger.warning("ClickHouse HTTP client not available")

CLICKHOUSE_AVAILABLE = CLICKHOUSE_NATIVE_AVAILABLE or CLICKHOUSE_HTTP_AVAILABLE
logger.info(f"ClickHouse enabled in config: {DB_CONFIG['enabled']}")
logger.info(f"ClickHouse configuration: {DB_CONFIG}")

# 内置HTTP客户端，避免循环导入
def execute_http_query(host, port, database, query, username, password, params=None, max_rows=10):
    """通过HTTP接口执行ClickHouse查询"""
        
    url = f"http://{host}:{port}/"
    
    # 处理参数化查询
    if params:
        try:
            # 替换查询中的参数占位符
            for key, value in params.items():
                placeholder = "{" + key + "}"
                if placeholder in query:
                    # 根据参数类型进行适当转换
                    if isinstance(value, str):
                        query = query.replace(placeholder, f"'{value}'")
                    else:
                        query = query.replace(placeholder, str(value))
        except Exception as e:
            logger.error(f"Error processing query parameters: {e}")
            return {
                "success": False,
                "data": None,
                "error": f"Error processing query parameters: {e}",
                "row_count": 0,
                "column_names": []
            }
    
    # 使用已验证工作的查询方式：URL参数认证 + GET请求
    params_dict = {
        "query": query,
        "user": username,
        "password": password,
        "database": database,
        "default_format": "JSONCompact"  # 使用JSON格式响应
    }
    
    try:
        logger.info(f"Executing ClickHouse HTTP query: {url}")
        response = requests.get(url, params=params_dict)
        response.raise_for_status()
        
        # 这里不使用外部func模块处理结果，直接简单处理
        content_type = response.headers.get('Content-Type', '')
        logger.info(f"Response content type: {content_type}")
        
        # 解析JSON响应
        if 'json' in content_type.lower():
            try:
                result = response.json()
                data = result.get('data', [])
                return {
                    "success": True,
                    "data": data,
                    "error": None,
                    "row_count": len(data),
                    "column_names": []
                }
            except Exception as json_err:
                logger.error(f"Failed to parse JSON response: {json_err}")
                return {
                    "success": False,
                    "data": None,
                    "error": f"Failed to parse JSON response: {json_err}",
                    "row_count": 0,
                    "column_names": []
                }
        
        # 如果不是JSON，按文本处理
        text = response.text.strip()
        return {
            "success": True,
            "data": [[text]],
            "error": None,
            "row_count": 1,
            "column_names": ["result"]
        }
    except Exception as e:
        logger.error(f"ClickHouse HTTP query error: {e}")
        return {
            "success": False,
            "data": None,
            "error": str(e),
            "row_count": 0,
            "column_names": []
        }

# 自定义HTTP连接类
class HTTPConnection:
    def __init__(self, host, port, database, username, password):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
    
    def execute(self, query, params=None):
        # 使用内置的HTTP查询函数，避免循环导入
        result = execute_http_query(
            self.host,
            self.port,
            self.database,
            query,
            self.username,
            self.password,
            params or {},
            DB_CONFIG["max_rows"]
        )
        if not result.get("success", False):
            raise Exception(result.get("error", "Unknown HTTP query error"))
        return result["data"]

# --- Lifespan Management ---
@asynccontextmanager
async def app_lifespan(app) -> AsyncIterator[None]:
    """Manage application lifecycle with database connection."""
    conn = None
    connection_mode = None
    
    logger.info("Starting app lifespan...")
    
    config = DB_CONFIG
    logger.info(f"ClickHouse connection enabled with config: {config}")
    
    # 1. 首先尝试HTTP连接（通过配置确定的优先方式）
    if CLICKHOUSE_HTTP_AVAILABLE:
        try:
            # 使用简单的HTTP请求测试连接
            url = f"http://{config['host']}:{config['http_port']}/"
            params = {
                "query": "SELECT 1",
                "user": config["username"],
                "password": config["password"],
                "database": config["database"]
            }
            
            logger.info(f"Attempting HTTP connection to {url} with params: {params}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            logger.info(f"ClickHouse HTTP connection successful: {response.text.strip()}")
            
            # 使用自定义的HTTP连接类
            http_conn = HTTPConnection(
                config['host'],
                config['http_port'],
                config['database'],
                config['username'],
                config['password']
            )
            
            # 测试执行一个简单查询
            try:
                test_result = http_conn.execute("SELECT 1")
                logger.info(f"HTTP connection execute test successful: {test_result}")
                
                # 使用try-except包裹DatabaseConnection初始化
                try:
                    conn = DatabaseConnection(
                        connection=http_conn,
                        database=config["database"],
                        connection_type="http"
                    )
                    connection_mode = "http"
                    logger.info("Successfully established ClickHouse HTTP connection")
                except Exception as model_err:
                    logger.error(f"Failed to create DatabaseConnection model: {model_err}")
                    logger.error(traceback.format_exc())
                    connection_mode = None
                    
            except Exception as exec_err:
                logger.warning(f"HTTP connection execute test failed: {exec_err}")
                logger.warning(traceback.format_exc())
                connection_mode = None
            
        except Exception as http_err:
            logger.warning(f"ClickHouse HTTP connection failed: {http_err}")
            logger.warning(traceback.format_exc())
            # 在HTTP失败后尝试原生连接，不退出
            connection_mode = None
    
    # 2. 如果HTTP连接失败或不可用，尝试原生驱动连接
    if connection_mode is None and CLICKHOUSE_NATIVE_AVAILABLE and ClickHouseClient is not None:
        try:
            logger.info(f"Testing ClickHouse native connection to {config['host']}:{config['port']}")
            
            client = ClickHouseClient(
                host=config["host"],
                port=config["port"],
                database=config["database"],
                user=config["username"],
                password=config["password"],
                settings={'readonly': 1}  # 强制只读模式
            )
            
            # 测试连接
            result = client.execute("SELECT 1")
            logger.info(f"ClickHouse native connection successful: {result}")
            
            # 使用try-except包裹DatabaseConnection初始化
            try:
                conn = DatabaseConnection(
                    connection=client,
                    database=config["database"],
                    connection_type="native"
                )
                connection_mode = "native"
                logger.info("Successfully established ClickHouse native connection")
            except Exception as model_err:
                logger.error(f"Failed to create DatabaseConnection model: {model_err}")
                logger.error(traceback.format_exc())
                connection_mode = None
            
        except Exception as native_err:
            logger.error(f"ClickHouse native connection failed: {native_err}")
            logger.error(traceback.format_exc())
            # 两种方式都失败了，将保持 conn=None
        
        # 记录最终连接状态
        if conn is None:
            logger.error("Failed to establish any ClickHouse connection")
            if not CLICKHOUSE_HTTP_AVAILABLE and not CLICKHOUSE_NATIVE_AVAILABLE:
                logger.error("Neither ClickHouse HTTP nor native drivers are available")
            else:
                logger.error("All connection attempts failed")
        else:
            logger.info(f"ClickHouse connection established using {connection_mode} mode")
            
        app_context.connection = conn
        app_context.connection_mode = connection_mode
    else:
        logger.warning("ClickHouse connection is disabled in configuration")

    try:
        logger.info("App is now running")
        yield # Server runs here
    finally:
        logger.info("App is shutting down")
        # Cleanup on shutdown
        if app_context.connection:
            logger.info(f"Closing ClickHouse {app_context.connection.connection_type} connection")
            
            if app_context.connection.connection_type == "native" and app_context.connection.connection:
                try:
                    # ClickHouse native client's cleanup
                    client = app_context.connection.connection
                    
                    # 尝试关闭底层网络连接
                    if hasattr(client, 'disconnect'):
                        client.disconnect()
                    elif hasattr(client, 'connection') and hasattr(client.connection, 'socket'):
                        client.connection.socket.close()
                    
                    logger.info("ClickHouse native connection closed")
                except Exception as e:
                    logger.error(f"Error closing ClickHouse native connection: {e}")
            
            # 确保连接对象被清除
            app_context.connection = None
            app_context.connection_mode = None
            logger.info("ClickHouse connection cleanup completed")
            