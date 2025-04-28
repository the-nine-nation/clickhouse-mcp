from setuptools import setup, find_packages

setup(
    name="clickhouse-readonly-mcp",
    version="0.1.0",
    description="A FastMCP server for executing read-only ClickHouse queries",
    author="Lucas Luo",
    author_email="your.email@example.com",
    packages=find_packages(),
    package_data={
        "clickhouse_mcp": ["*.py"],
    },
    install_requires=[
        "fastmcp",
        "pydantic",
        "requests",
        "clickhouse_driver",
    ],
    entry_points={
        "console_scripts": [
            "clickhouse-mcp=clickhouse_mcp.main:main",
        ],
    },
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
) 