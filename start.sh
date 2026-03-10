#!/bin/bash
# 启动 MySQL MCP Server

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Starting MySQL MCP Server"
echo "=========================="

if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

if ! python3 -c "import mcp" 2>/dev/null; then
    echo "Installing mcp package..."
    pip install mcp
fi

if ! python3 -c "import pymysql" 2>/dev/null; then
    echo "Installing pymysql package..."
    pip install pymysql
fi

python3 mysql_exec_server.py
