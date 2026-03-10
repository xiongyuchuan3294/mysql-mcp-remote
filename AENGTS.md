# MySQL MCP Remote

当前项目基于 `pymysql` 连接 MySQL，并通过 MCP 暴露 SQL 工具。

## 项目结构

- `mysql_exec_server.py`: MCP server 入口
- `mysql_client.py`: MySQL 连接与 SQL 执行封装
- `conf/aml_conf.conf`: MySQL 连接配置
- `test_remote_connection.py`: 连通性测试脚本（TCP + SQL）
- `start.sh`: 启动脚本

## 配置说明

`conf/aml_conf.conf` 中每个连接项支持两种格式：

- `host,port,database,user,password`
- `host,port,database,user,password,charset`

示例：

```ini
[mysql]
demo_db = 127.0.0.1,3307,demo_db,dev,123456
```

默认连接配置由代码内常量提供：

- `MYSQL_MCP_ENV_NAME`（默认：`MySQL MCP Server - Remote`）
- `MYSQL_MCP_DEFAULT_CONF`（默认：`demo_db`）
- `MYSQL_MCP_CONF_HEADER`（默认：`mysql`）
- `MYSQL_MCP_CONF_FILE`（默认：`aml_conf.conf`）

如需临时覆盖，可在启动前设置环境变量。

## 连通性测试

默认测试（连接 `default_conf`，执行 `SELECT 1 AS ok`）：

```bash
python3 test_remote_connection.py
```

指定连接键和 SQL：

```bash
python3 test_remote_connection.py demo_db "SHOW TABLES"
```

返回码：

- `0`: TCP 与 SQL 均成功
- `1`: TCP 不通
- `2`: TCP 通但 SQL 失败
- `3`: 配置解析失败

## 启动 MCP Server

```bash
python3 mysql_exec_server.py
```

或：

```bash
./start.sh
```

## MCP 配置示例

```json
{
  "mcpServers": {
    "mysql-exec-server": {
      "command": "python3",
      "args": ["/Users/xiongyuc/workspace/mysql-mcp-remote/mysql_exec_server.py"]
    }
  }
}
```

## 可用工具

- `mysql_execute_query`: 执行查询并返回 TSV
- `mysql_describe_table`: 查看表结构
- `mysql_count_records`: 统计行数（可加 where 条件）
- `mysql_preview_data`: 预览数据（默认 limit 10）
- `mysql_execute_dml`: 执行 DDL/DML
- `mysql_show_tables`: 查看当前库下所有表
- `mysql_close_connections`: 关闭连接（当前实现为 no-op）
