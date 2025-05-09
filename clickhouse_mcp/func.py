import json
import requests
import logging

logger = logging.getLogger(__name__)

# --- ClickHouse HTTP Client ---
def execute_http_query(host, port, database, query, username, password, params=None, max_rows=10):
    """通过HTTP接口执行ClickHouse查询"""
        
    url = f"http://{host}:{port}/"
    
    # 处理参数化查询
    if params:
        try:
            # 使用单次遍历替换所有参数占位符
            for key, value in params.items():
                placeholder = "{" + key + "}"
                if placeholder in query:
                    query = query.replace(placeholder, f"'{value}'" if isinstance(value, str) else str(value))
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
        "default_format": "JSONCompact",  # 使用JSON格式响应
        "max_result_rows": str(max_rows)  # 在查询级别限制结果行数，减少数据传输
    }
    
    try:
        # 设置超时，避免长时间等待
        response = requests.get(url, params=params_dict, timeout=30)
        response.raise_for_status()
        return process_clickhouse_response(response, max_rows)
    except Exception as e:
        logger.error(f"ClickHouse HTTP query error: {e}")
        return {
            "success": False,
            "data": None,
            "error": str(e),
            "row_count": 0,
            "column_names": []
        }

def process_clickhouse_response(response, max_rows):
    """处理ClickHouse HTTP响应"""
    try:
        content_type = response.headers.get('Content-Type', '').lower()
        
        # 尝试解析JSON响应 - 最常见和最高效的路径优先
        if 'json' in content_type:
            try:
                result = response.json()
                return process_clickhouse_result(result, max_rows)
            except json.JSONDecodeError:
                # 失败后继续处理为文本
                pass
        
        # 处理TSV格式（ClickHouse默认）
        if 'text/tab-separated-values' in content_type or 'tsv' in content_type:
            text = response.text.strip()
            if not text:
                return {"success": True, "data": [], "error": None, "row_count": 0, "column_names": []}
                
            # 分割成行
            lines = text.split('\n')
            
            # 单值响应的快速路径
            if len(lines) == 1 and '\t' not in lines[0]:
                return {
                    "success": True,
                    "data": [[lines[0]]],
                    "error": None,
                    "row_count": 1,
                    "column_names": ["result"]
                }
            
            # 处理多行TSV数据
            rows = []
            column_names = ["value"] if lines else []
            
            for line in lines:
                if not line.strip():
                    continue
                
                if '\t' in line:
                    row_values = line.split('\t')
                    rows.append(row_values)
                    # 只在第一次遇到多列时生成列名
                    if len(column_names) == 1 and len(column_names) < len(row_values):
                        column_names = [f"column_{i+1}" for i in range(len(row_values))]
                else:
                    rows.append([line])
            
            return {
                "success": True,
                "data": rows[:max_rows],
                "error": None,
                "row_count": len(rows),
                "column_names": column_names
            }
                
        # 其他文本响应 - 简化处理
        text = response.text.strip()
        
        if not text:
            return {"success": True, "data": [], "error": None, "row_count": 0, "column_names": []}
            
        # 快速处理单行响应
        if '\n' not in text:
            return {
                "success": True,
                "data": [[text]],
                "error": None,
                "row_count": 1,
                "column_names": ["result"]
            }
            
        # 处理多行响应
        lines = [line for line in text.split('\n') if line.strip()]
        
        return {
            "success": True,
            "data": [[line] for line in lines[:max_rows]],
            "error": None,
            "row_count": len(lines),
            "column_names": ["result"]
        }
            
    except Exception as e:
        logger.error(f"Error processing response: {e}")
        return {
            "success": False,
            "data": None,
            "error": str(e),
            "row_count": 0,
            "column_names": []
        }

def process_clickhouse_result(result, max_rows):
    """处理ClickHouse HTTP响应结果"""
    # 处理结果
    if "data" not in result:
        return {
            "success": True,
            "data": [],
            "error": None,
            "row_count": 0,
            "column_names": []
        }
    
    rows = result.get("data", [])
    column_names = []
    
    # 从meta字段获取列名 - 更快的路径
    if "meta" in result:
        column_names = [col.get("name") for col in result.get("meta", [])]
    elif rows:
        # 如果没有meta，从第一行获取列名
        column_names = list(rows[0].keys()) if hasattr(rows[0], 'keys') else []
        
    return {
        "success": True,
        "data": rows[:max_rows],
        "error": None,
        "row_count": len(rows),
        "column_names": column_names
    }
        
def process_native_result(result_set, query_lower, max_rows):
    """处理原生客户端查询结果"""
    # 快速路径 - 空结果集
    if not result_set:
        return {
            "success": True,
            "data": [],
            "error": None,
            "row_count": 0,
            "column_names": []
        }
    
    # 快速路径 - 非列表结果（可能是行数）
    if not isinstance(result_set, list):
        return {
            "success": True,
            "data": [[str(result_set)]],
            "error": None,
            "row_count": 1,
            "column_names": ["result"]
        }
    
    # 快速路径 - 空列表
    if not result_set:
        return {
            "success": True,
            "data": [],
            "error": None,
            "row_count": 0,
            "column_names": []
        }
    
    # 判断查询类型
    is_show_query = query_lower.startswith(("show", "describe", "desc"))
    column_names = []
    
    # 对于SHOW/DESCRIBE等简单查询
    if is_show_query:
        if isinstance(result_set[0], (list, tuple)):
            # 生成默认列名
            if query_lower.startswith("show tables"):
                column_names = ["table_name"]
            elif query_lower.startswith(("describe", "desc")):
                column_names = ["name", "type", "default_type", "default_expression"]
            else:
                column_names = [f"column_{i}" for i in range(len(result_set[0]))]
            
            return {
                "success": True,
                "data": result_set[:max_rows],
                "error": None,
                "row_count": len(result_set),
                "column_names": column_names
            }
        else:
            # 对于返回标量列表的情况
            return {
                "success": True,
                "data": [[item] for item in result_set[:max_rows]],
                "error": None,
                "row_count": len(result_set),
                "column_names": ["value"]
            }
    
    # 对于SELECT查询
    if hasattr(result_set[0], 'keys'):
        # 结果是字典列表
        column_names = list(result_set[0].keys())
        return {
            "success": True,
            "data": result_set[:max_rows],
            "error": None,
            "row_count": len(result_set),
            "column_names": column_names
        }
    else:
        # 结果是元组/列表列表
        num_cols = len(result_set[0]) if isinstance(result_set[0], (list, tuple)) else 1
        column_names = [f"column_{i}" for i in range(num_cols)]
        return {
            "success": True,
            "data": result_set[:max_rows],
            "error": None,
            "row_count": len(result_set),
            "column_names": column_names
        }

def format_query_results(result) -> str:
    """格式化查询结果为字符串，使用表格格式提高可读性"""
    # 快速处理错误和空结果
    if not result.get("success"):
        return f"Error executing query: {result.get('error', 'Unknown error')}"
    if result.get("error"):
        return f"Error: {result.get('error')}"
    if not result.get("data"):
        return f"Query executed. Rows returned: {result.get('row_count', 0)}"
    if not result["data"]:
        return "Query executed. No data returned."
    
    # 获取数据和列名
    data = result["data"]
    column_names = result.get("column_names", [])
    
    # 准备数据行
    rows = []
    if column_names:
        rows.append(column_names)
    
    # 处理各种格式的数据
    if isinstance(data[0], dict):
        # 字典数据
        for row in data:
            rows.append([row.get(col, '') for col in column_names])
    elif isinstance(data[0], list):
        # 列表数据
        rows.extend(data)
    else:
        # 单值数据
        rows.extend([[item] for item in data])
    
    # 计算每列的最大宽度
    if not rows:
        return "Query executed. No data to display."
    
    # 将所有数据转换为字符串
    str_rows = [[str(cell) for cell in row] for row in rows]
    
    # 计算每列宽度（最小宽度为列名长度）
    col_widths = []
    for i in range(len(str_rows[0])):
        col_widths.append(max(len(row[i]) for row in str_rows if i < len(row)))
    
    # 创建格式化字符串
    row_format = "| " + " | ".join("{:<" + str(width) + "}" for width in col_widths) + " |"
    
    # 创建分隔行
    separator = "+" + "+".join("-" * (width + 2) for width in col_widths) + "+"
    separator = separator.replace("--", "--")
    
    # 构建结果表格
    output_lines = [separator]
    
    # 添加标题行和标题分隔行（如果有列名）
    if column_names:
        output_lines.append(row_format.format(*str_rows[0]))
        output_lines.append(separator)
        data_rows = str_rows[1:]
    else:
        data_rows = str_rows
    
    # 添加数据行
    for row in data_rows:
        # 确保行有足够的单元格数量
        while len(row) < len(col_widths):
            row.append("")
        output_lines.append(row_format.format(*row))
    
    # 添加底部分隔行和摘要
    output_lines.append(separator)
    output_lines.append(f"Total rows: {result['row_count']} (showing first {len(data)})")
    
    return "\n".join(output_lines)