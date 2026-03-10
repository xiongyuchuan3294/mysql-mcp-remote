#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Smoke test MySQL connectivity (TCP + SQL)."""

from __future__ import annotations

import socket
import sys

from mysql_client import (
    DEFAULT_CONF_FILE,
    DEFAULT_CONF_HEADER,
    DEFAULT_CONF_VALUE,
    ENV_NAME,
    MysqlRuntime,
    parse_mysql_conf,
)


def tcp_probe(host: str, port: int, timeout: float = 3.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, ""
    except Exception as exc:
        return False, str(exc)


def main() -> int:
    conf_value = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_CONF_VALUE
    sql = sys.argv[2] if len(sys.argv) > 2 else "SELECT 1 AS ok"

    try:
        conn_conf = parse_mysql_conf(
            conf_value=conf_value,
            conf_header=DEFAULT_CONF_HEADER,
            conf_file=DEFAULT_CONF_FILE,
        )
    except Exception as exc:
        print(f"[FAIL] Invalid mysql config: {exc}")
        return 3

    print(f"[INFO] Environment: {ENV_NAME}")
    print(f"[INFO] Config key/raw: {conf_value}")
    print(f"[INFO] Target: {conn_conf.host}:{conn_conf.port}/{conn_conf.database}")

    reachable, error = tcp_probe(conn_conf.host, conn_conf.port)
    if not reachable:
        print(f"[FAIL] TCP connect failed: {error}")
        print("[HINT] Check host/port and network route.")
        print("[HINT] If you use SSH tunnel, ensure tunnel is running first.")
        return 1

    print("[PASS] TCP port is reachable.")

    try:
        result = MysqlRuntime.execute_query_tsv(
            sql=sql,
            conf_value=conf_value,
            conf_header=DEFAULT_CONF_HEADER,
            conf_file=DEFAULT_CONF_FILE,
        )
    except Exception as exc:
        print(f"[FAIL] MySQL query failed: {exc}")
        print("[HINT] TCP is up but account/schema/sql may be invalid.")
        return 2

    print("[PASS] MySQL query succeeded.")
    print("[RESULT]")
    print(result if result.strip() else "<empty>")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
