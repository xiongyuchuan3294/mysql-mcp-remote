#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MySQL execution MCP server."""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from mcp.server.fastmcp import FastMCP

from mysql_client import (
    DEFAULT_CONF_FILE,
    DEFAULT_CONF_HEADER,
    DEFAULT_CONF_VALUE,
    ENV_NAME,
    MysqlRuntime,
)


logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

mcp = FastMCP(f"mysql_exec_server_{ENV_NAME}")

_IDENTIFIER_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$"
)


def _validate_identifier(name: str, field_name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {field_name}: {name}")
    return name


def _query_error(exc: Exception) -> str:
    LOGGER.error("MySQL request failed: %s", exc)
    return f"查询失败: {exc}"


@mcp.tool(
    name="mysql_execute_query",
    description=(
        "Execute MySQL SQL query and return tab-separated results. "
        f"Environment: {ENV_NAME}"
    ),
)
def mysql_execute_query(
    sql: str,
    conf_value: str = DEFAULT_CONF_VALUE,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
    fetch_size: int | None = None,
) -> str:
    """Execute MySQL query and return TSV result."""
    try:
        LOGGER.info(
            "Execute MySQL query. conf=%s sql=%s",
            conf_value,
            sql[:100],
        )
        return MysqlRuntime.execute_query_tsv(
            sql=sql,
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
            fetch_size=fetch_size,
        )
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="mysql_describe_table",
    description=f"Describe a MySQL table. Environment: {ENV_NAME}",
)
def mysql_describe_table(
    table_name: str,
    conf_value: str = DEFAULT_CONF_VALUE,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> str:
    """Describe a table structure."""
    try:
        safe_table_name = _validate_identifier(table_name, "table_name")
        sql = f"DESCRIBE {safe_table_name}"
        LOGGER.info("Describe MySQL table. conf=%s table=%s", conf_value, safe_table_name)
        return MysqlRuntime.execute_query_tsv(
            sql=sql,
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
        )
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="mysql_count_records",
    description=f"Count rows in a MySQL table. Environment: {ENV_NAME}",
)
def mysql_count_records(
    table_name: str,
    where_clause: str | None = None,
    conf_value: str = DEFAULT_CONF_VALUE,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> str:
    """Count records in a table."""
    try:
        safe_table_name = _validate_identifier(table_name, "table_name")
        sql = f"SELECT COUNT(*) AS record_count FROM {safe_table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        LOGGER.info(
            "Count MySQL records. conf=%s table=%s where=%s",
            conf_value,
            safe_table_name,
            where_clause,
        )
        return MysqlRuntime.execute_query_tsv(
            sql=sql,
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
        )
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="mysql_preview_data",
    description=f"Preview MySQL table data. Environment: {ENV_NAME}",
)
def mysql_preview_data(
    table_name: str,
    where_clause: str | None = None,
    limit: int = 10,
    conf_value: str = DEFAULT_CONF_VALUE,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> str:
    """Preview table data."""
    try:
        safe_table_name = _validate_identifier(table_name, "table_name")
        safe_limit = max(1, min(int(limit), 1000))
        sql = f"SELECT * FROM {safe_table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        sql += f" LIMIT {safe_limit}"
        LOGGER.info(
            "Preview MySQL data. conf=%s table=%s where=%s limit=%s",
            conf_value,
            safe_table_name,
            where_clause,
            safe_limit,
        )
        return MysqlRuntime.execute_query_tsv(
            sql=sql,
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
        )
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="mysql_execute_dml",
    description=f"Execute MySQL DDL/DML. Environment: {ENV_NAME}",
)
def mysql_execute_dml(
    sql: str,
    conf_value: str = DEFAULT_CONF_VALUE,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> str:
    """Execute DDL or DML statement."""
    try:
        LOGGER.info("Execute MySQL DML. conf=%s sql=%s", conf_value, sql[:100])
        affected_rows = MysqlRuntime.execute_dml(
            sql=sql,
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
        )
        return f"执行成功，影响行数: {affected_rows}"
    except Exception as exc:
        LOGGER.error("MySQL DML failed: %s", exc)
        return f"执行失败: {exc}"


@mcp.tool(
    name="mysql_show_tables",
    description=f"Show tables in current MySQL database. Environment: {ENV_NAME}",
)
def mysql_show_tables(
    conf_value: str = DEFAULT_CONF_VALUE,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> str:
    """Show all tables in current database."""
    try:
        LOGGER.info("Show MySQL tables. conf=%s", conf_value)
        return MysqlRuntime.execute_query_tsv(
            sql="SHOW TABLES",
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
        )
    except Exception as exc:
        return _query_error(exc)


@mcp.tool(
    name="mysql_close_connections",
    description="Close cached MySQL connections.",
)
def mysql_close_connections() -> str:
    """Close cached connections."""
    try:
        LOGGER.info("Close MySQL connections")
        MysqlRuntime.close_all()
        return "所有连接已关闭"
    except Exception as exc:
        LOGGER.error("Close MySQL connections failed: %s", exc)
        return f"关闭连接失败: {exc}"


if __name__ == "__main__":
    LOGGER.info("Start MySQL execution MCP server. Environment: %s", ENV_NAME)
    mcp.run()
