#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""MySQL connection and execution helpers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Iterable

import pymysql

from conf.config import get_config


LOGGER = logging.getLogger("mysql_client")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)

ENV_NAME = os.environ.get("MYSQL_MCP_ENV_NAME", "MySQL MCP Server - Remote")
DEFAULT_CONF_VALUE = os.environ.get("MYSQL_MCP_DEFAULT_CONF", "demo_db")
DEFAULT_CONF_HEADER = os.environ.get("MYSQL_MCP_CONF_HEADER", "mysql")
DEFAULT_CONF_FILE = os.environ.get("MYSQL_MCP_CONF_FILE", "aml_conf.conf")


@dataclass(frozen=True)
class MysqlConnectionConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    charset: str = "utf8mb4"


def resolve_mysql_conf(
    conf_value: str,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> str:
    """Resolve either a raw connection string or a named profile."""
    if "," in conf_value:
        return conf_value
    return get_config(conf_value, conf_header=conf_header, conf_file=conf_file)


def parse_mysql_conf(
    conf_value: str,
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> MysqlConnectionConfig:
    """Parse MySQL config from profile name or raw string."""
    resolved = resolve_mysql_conf(conf_value, conf_header=conf_header, conf_file=conf_file)
    parts = [item.strip() for item in resolved.split(",")]
    if len(parts) == 5:
        parts.append("utf8mb4")
    if len(parts) != 6:
        raise ValueError(
            "Invalid mysql config, expected "
            "'host,port,database,user,password[,charset]'"
        )
    host, port, database, user, password, charset = parts
    return MysqlConnectionConfig(
        host=host,
        port=int(port),
        database=database,
        user=user,
        password=password,
        charset=charset,
    )


def _sanitize_tsv_value(value: Any) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    return text.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def rows_to_tsv(rows: list[dict[str, Any]]) -> str:
    """Render query rows to TSV text."""
    if not rows:
        return ""
    columns = list(rows[0].keys())
    lines = ["\t".join(columns)]
    for row in rows:
        lines.append("\t".join(_sanitize_tsv_value(row.get(col)) for col in columns))
    return "\n".join(lines)


class MysqlRuntime:
    """Runtime helpers used by MCP server and scripts."""

    @staticmethod
    def _connect(
        conf_value: str,
        conf_header: str = DEFAULT_CONF_HEADER,
        conf_file: str = DEFAULT_CONF_FILE,
    ) -> pymysql.connections.Connection:
        config = parse_mysql_conf(conf_value, conf_header=conf_header, conf_file=conf_file)
        LOGGER.info(
            "Connecting to MySQL %s:%s/%s",
            config.host,
            config.port,
            config.database,
        )
        return pymysql.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
            charset=config.charset,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    @classmethod
    def execute_query(
        cls,
        sql: str,
        conf_value: str = DEFAULT_CONF_VALUE,
        conf_header: str = DEFAULT_CONF_HEADER,
        conf_file: str = DEFAULT_CONF_FILE,
        fetch_size: int | None = None,
    ) -> list[dict[str, Any]]:
        """Execute SELECT-like SQL and return rows."""
        connection = cls._connect(conf_value, conf_header=conf_header, conf_file=conf_file)
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                if isinstance(fetch_size, int) and fetch_size > 0:
                    return cursor.fetchmany(fetch_size)
                return cursor.fetchall()
        finally:
            connection.close()

    @classmethod
    def execute_query_tsv(
        cls,
        sql: str,
        conf_value: str = DEFAULT_CONF_VALUE,
        conf_header: str = DEFAULT_CONF_HEADER,
        conf_file: str = DEFAULT_CONF_FILE,
        fetch_size: int | None = None,
    ) -> str:
        """Execute query and return TSV output."""
        rows = cls.execute_query(
            sql=sql,
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
            fetch_size=fetch_size,
        )
        return rows_to_tsv(rows)

    @classmethod
    def execute_dml(
        cls,
        sql: str,
        conf_value: str = DEFAULT_CONF_VALUE,
        conf_header: str = DEFAULT_CONF_HEADER,
        conf_file: str = DEFAULT_CONF_FILE,
    ) -> int:
        """Execute DDL/DML SQL and return affected row count."""
        connection = cls._connect(conf_value, conf_header=conf_header, conf_file=conf_file)
        try:
            with connection.cursor() as cursor:
                affected_rows = cursor.execute(sql)
                connection.commit()
                return affected_rows
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def close_all() -> None:
        """Keep API compatible with Hive implementation."""
        return None


class Mysql:
    """Backward-compatible client with query and commit helpers."""

    def __init__(
        self,
        conf_value: str,
        conf_header: str = DEFAULT_CONF_HEADER,
        conf_file: str = DEFAULT_CONF_FILE,
    ):
        self._conf_value = conf_value
        self._conf_header = conf_header
        self._conf_file = conf_file
        self._connection = MysqlRuntime._connect(
            conf_value=conf_value,
            conf_header=conf_header,
            conf_file=conf_file,
        )
        self._cursor = self._connection.cursor()

    def query(self, sql: str, num: int | str = "") -> list[dict[str, Any]]:
        """Execute a SELECT-like statement."""
        try:
            self._cursor.execute(sql)
            if isinstance(num, int):
                return self._cursor.fetchmany(num)
            if isinstance(num, str) and num.isdigit():
                return self._cursor.fetchmany(int(num))
            return self._cursor.fetchall()
        except Exception:
            self._connection.rollback()
            LOGGER.exception("MySQL query failed")
            return []
        finally:
            self.close()

    def commit(self, sql: str) -> None:
        """Execute a write statement."""
        try:
            self._cursor.execute(sql)
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            LOGGER.exception("MySQL commit failed")
            raise
        finally:
            self.close()

    def insert_sql(self, table_name: str, data: list[dict[str, Any]]) -> None:
        """Insert rows by composing a single INSERT statement."""
        if not data:
            LOGGER.warning("Skip empty insert for table %s", table_name)
            return

        columns = list(data[0].keys())
        rendered_rows = []
        for row in data:
            values = []
            for column in columns:
                value = row.get(column)
                if value is None:
                    values.append("NULL")
                elif isinstance(value, str):
                    values.append("'" + value.replace("'", "''") + "'")
                else:
                    values.append(str(value))
            rendered_rows.append("(" + ",".join(values) + ")")

        sql = (
            f"INSERT INTO {table_name}({','.join(columns)}) VALUES\n"
            + ",\n".join(rendered_rows)
            + ";"
        )
        self.commit(sql)

    def execute_many(self, sql: str, params_list: Iterable[tuple]) -> None:
        """Execute a parameterized batch statement."""
        try:
            self._cursor.executemany(sql, list(params_list))
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            LOGGER.exception("MySQL batch execute failed")
            raise
        finally:
            self.close()

    def close(self) -> None:
        if getattr(self, "_cursor", None):
            self._cursor.close()
            self._cursor = None
        if getattr(self, "_connection", None):
            self._connection.close()
            self._connection = None


def op_mysql(
    conf_value: str,
    sql: str,
    op_type: str = "query",
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
):
    client = Mysql(conf_value, conf_header=conf_header, conf_file=conf_file)
    if op_type == "query":
        return client.query(sql)
    if op_type == "commit":
        client.commit(sql)
        return None
    raise ValueError("op_type must be 'query' or 'commit'")


def insert_mysql(
    conf_value: str,
    table_name: str,
    data: list[dict[str, Any]],
    conf_header: str = DEFAULT_CONF_HEADER,
    conf_file: str = DEFAULT_CONF_FILE,
) -> None:
    client = Mysql(conf_value, conf_header=conf_header, conf_file=conf_file)
    client.insert_sql(table_name, data)
