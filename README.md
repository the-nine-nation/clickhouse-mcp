# ClickHouse Readonly MCP

一个用于执行只读ClickHouse查询的FastMCP服务器，支持本地Python API集成和作为独立服务运行。

## 更新

**20250509**:添加了对官方的clickhouse mcp的代码,并进行了优化,避免多次调用的思路回环.

## 功能特点

- 支持通过MCP工具执行只读SQL查询
- 输入验证确保只能执行SELECT、SHOW、DESCRIBE、EXPLAIN等只读操作
- 支持HTTP和原生ClickHouse连接方式，自动故障切换
- 简洁的表格格式输出结果
- 支持参数化查询
- 高性能查询执行和结果处理

## 安装

### 通过pip安装

```bash
pip install clickhouse-readonly-mcp
```

### 从源码安装

```bash
git clone https://github.com/the-nine-nation/clickhouse-mcp.git
cd clickhouse-readonly-mcp
pip install -e .
```

## 使用方法

以cursor为例,将如下字典放入config.json:
其中 sys.executable为python虚拟环境的执行文件,通常名字为python,conda或uv下皆可以
clickhouse_mcp_py为main.py的绝对路径
```json
    "clickhouse": {
        "command": sys.executable,
        "args": [clickhouse_mcp_py],
        "env": {
            "CLICKHOUSE_ENABLED": "true",
            "CLICKHOUSE_HOST": "ClickHouse database IP",
            "CLICKHOUSE_PORT": "ClickHouse database port",
            "CLICKHOUSE_HTTP_PORT":"",
            "CLICKHOUSE_DATABASE": "ClickHouse database name",
            "CLICKHOUSE_USERNAME": "ClickHouse database username",
            "CLICKHOUSE_PASSWORD": "ClickHouse database password",
            "CLICKHOUSE_RESOURCE_DESC_FILE": "Path to ClickHouse database resource description file"
            }
    }
```
或者使用经过我们优化的官方实现:
```json
    "clickhouse": {
        "command": sys.executable,
        "args": [clickhouse_mcp_py],
    "env": {
        "CLICKHOUSE_HOST": "<clickhouse-host>",
        "CLICKHOUSE_PORT": "<clickhouse-port>",
        "CLICKHOUSE_USER": "<clickhouse-user>",
        "CLICKHOUSE_PASSWORD": "<clickhouse-password>",
        "CLICKHOUSE_SECURE": "true",
        "CLICKHOUSE_VERIFY": "true",
        "CLICKHOUSE_CONNECT_TIMEOUT": "30",
        "CLICKHOUSE_SEND_RECEIVE_TIMEOUT": "30"
      }
```

请注意:
1.CLICKHOUSE_PORT为原生连接的端口,CLICKHOUSE_HTTP_PORT为http协议端口,该mcp会自动切换,不一定需要全部填写;
2.CLICKHOUSE_RESOURCE_DESC_FILE是一个说明,可以将数据库中一些信息放入其中,例如什么表是做什么用的,能够提升模型理解能力.
3.CLICKHOUSE_ENABLED默认可以不用填



## 许可证

MIT
