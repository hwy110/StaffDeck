#!/usr/bin/env python3
"""SQLite → PostgreSQL 数据迁移脚本 — StaffDeck

将现有 SQLite 数据库中的所有数据迁移到 PostgreSQL。

用法:
    # 1. 先在 PG 中创建表结构
    DATABASE_URL="postgresql://user:pass@host:5432/staffdeck" \\
        python -m alembic -c alembic.ini upgrade head

    # 2. 运行数据迁移
    python migrate_sqlite_to_pg.py \\
        --sqlite ./skill_agent_loop.db \\
        --pg-url "postgresql://user:pass@host:5432/staffdeck"

    # 3. 或者通过环境变量
    SQLITE_PATH=./skill_agent_loop.db \\
    DATABASE_URL="postgresql://user:pass@host:5432/staffdeck" \\
        python migrate_sqlite_to_pg.py

注意:
    - 迁移前请确保 PostgreSQL 数据库已通过 Alembic 初始化了表结构
    - 迁移脚本会跳过已存在的表
    - 建议先在测试环境验证
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.engine import Engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 所有需要迁移的表（按依赖顺序排列，先迁移被引用的表）────────────
TABLE_ORDER = [
    "tenants",
    "users",
    "user_avatars",
    "agent_profiles",
    "model_configs",
    "persona_configs",
    "ui_configs",
    "skills",
    "skill_versions",
    "general_skills",
    "knowledge_bases",
    "knowledge_base_versions",
    "knowledge_documents",
    "knowledge_buckets",
    "knowledge_chunks",
    "knowledge_concepts",
    "knowledge_discovery_suggestions",
    "knowledge_ingest_jobs",
    "agent_skill_branches",
    "agent_skill_branch_versions",
    "agent_knowledge_branches",
    "agent_usages",
    "agent_model_bindings",
    "agent_resource_bindings",
    "tools",
    "mcp_servers",
    "mock_orders",
    "sessions",
    "messages",
    "message_feedback",
    "skill_feedback",
    "agent_events",
    "memories",
    "channel_bindings",
    "channel_binding_agents",
    "channel_conv_states",
    "channel_bind_codes",
    "channel_identities",
    "channel_inbound_events",
    "channel_deliveries",
    "human_handoff_requests",
    "scheduled_tasks",
    "scheduled_task_runs",
    "harness_task_frames",
    "harness_runs",
    "harness_turns",
    "harness_session_leases",
    "harness_invocations",
]


def create_sqlite_engine(sqlite_path: str) -> Engine:
    """创建 SQLite 引擎。"""
    resolved = Path(sqlite_path).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"SQLite 数据库不存在: {resolved}")
    logger.info("SQLite 数据库: %s (%.2f MB)", resolved, resolved.stat().st_size / 1024 / 1024)
    return create_engine(f"sqlite:///{resolved}")


def create_pg_engine(pg_url: str) -> Engine:
    """创建 PostgreSQL 引擎。"""
    if not pg_url.startswith("postgresql"):
        raise ValueError(f"DATABASE_URL 必须是 postgresql:// 开头, 当前: {pg_url[:20]}...")
    logger.info("PostgreSQL 目标: %s", pg_url.split("@")[-1] if "@" in pg_url else pg_url)
    return create_engine(pg_url, pool_pre_ping=True)


def get_pg_tables(pg_engine: Engine) -> set[str]:
    """获取 PostgreSQL 中已存在的表名。"""
    return set(inspect(pg_engine).get_table_names())


def migrate_table(
    sqlite_engine: Engine,
    pg_engine: Engine,
    table_name: str,
    batch_size: int = 500,
) -> int:
    """迁移单个表的数据，返回迁移行数。"""
    sqlite_meta = MetaData()
    sqlite_meta.reflect(bind=sqlite_engine, only=[table_name])
    if table_name not in sqlite_meta.tables:
        logger.warning("  跳过: SQLite 中不存在表 %s", table_name)
        return 0

    sqlite_table = sqlite_meta.tables[table_name]

    # 读取 SQLite 数据
    with sqlite_engine.connect() as src_conn:
        rows = src_conn.execute(sqlite_table.select()).fetchall()

    if not rows:
        logger.info("  %s: 0 行（空表）", table_name)
        return 0

    columns = [col.name for col in sqlite_table.columns]
    total = len(rows)
    inserted = 0

    with pg_engine.begin() as dst_conn:
        # 检查目标表是否已有数据
        existing = dst_conn.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        ).scalar()
        if existing and existing > 0:
            logger.info("  %s: 目标已有 %d 行，跳过", table_name, existing)
            return 0

        for i in range(0, total, batch_size):
            batch = rows[i : i + batch_size]
            # 构建参数化 INSERT
            values_clause = ", ".join(f":{col}" for col in columns)
            col_list = ", ".join(columns)
            insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({values_clause})"

            for row in batch:
                params = {}
                for col_name, value in zip(columns, row):
                    # SQLite 中 JSON 列可能以字符串形式存储
                    params[col_name] = value
                dst_conn.execute(text(insert_sql), params)
                inserted += 1

            if inserted % 2000 == 0 or i + batch_size >= total:
                logger.info("  %s: %d / %d 行", table_name, inserted, total)

    return inserted


def migrate_all(
    sqlite_path: str,
    pg_url: str,
    tables: list[str] | None = None,
    batch_size: int = 500,
    dry_run: bool = False,
) -> dict[str, int]:
    """执行完整迁移，返回 {表名: 行数}。"""
    sqlite_engine = create_sqlite_engine(sqlite_path)
    pg_engine = create_pg_engine(pg_url)

    # 获取 SQLite 中实际存在的表
    sqlite_tables = set(inspect(sqlite_engine).get_table_names())
    pg_tables = get_pg_tables(pg_engine)

    # 确定迁移顺序
    ordered = tables or TABLE_ORDER
    to_migrate = [t for t in ordered if t in sqlite_tables and t in pg_tables]
    skipped_sqlite = sqlite_tables - set(ordered)
    skipped_pg = set(ordered) - pg_tables

    if skipped_sqlite:
        logger.info("SQLite 中未在迁移列表中的表: %s", ", ".join(sorted(skipped_sqlite)))
    if skipped_pg:
        logger.warning("PostgreSQL 中缺少表（需先运行 Alembic）: %s", ", ".join(sorted(skipped_pg)))

    results: dict[str, int] = {}

    if dry_run:
        logger.info("[DRY RUN] 将迁移以下 %d 个表:", len(to_migrate))
        for t in to_migrate:
            with sqlite_engine.connect() as conn:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            logger.info("  %s: %d 行", t, count)
            results[t] = count
        return results

    logger.info("开始迁移 %d 个表...", len(to_migrate))
    start = time.monotonic()

    for table_name in to_migrate:
        try:
            count = migrate_table(sqlite_engine, pg_engine, table_name, batch_size)
            results[table_name] = count
        except Exception as exc:
            logger.error("迁移 %s 失败: %s", table_name, exc)
            results[table_name] = -1

    elapsed = time.monotonic() - start
    total_rows = sum(v for v in results.values() if v > 0)
    failed = [t for t, v in results.items() if v < 0]

    logger.info("=" * 60)
    logger.info("迁移完成: %d 行数据, 耗时 %.1f 秒", total_rows, elapsed)
    if failed:
        logger.error("失败的表: %s", ", ".join(failed))
    logger.info("=" * 60)

    sqlite_engine.dispose()
    pg_engine.dispose()
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="StaffDeck SQLite → PostgreSQL 数据迁移")
    parser.add_argument(
        "--sqlite",
        default=os.environ.get("SQLITE_PATH", "./skill_agent_loop.db"),
        help="SQLite 数据库路径（默认: ./skill_agent_loop.db）",
    )
    parser.add_argument(
        "--pg-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="PostgreSQL 连接 URL（默认: $DATABASE_URL）",
    )
    parser.add_argument("--batch-size", type=int, default=500, help="批量插入大小")
    parser.add_argument("--dry-run", action="store_true", help="仅显示将迁移的数据量")
    parser.add_argument("--tables", nargs="*", help="仅迁移指定表")
    args = parser.parse_args()

    if not args.pg_url:
        parser.error("必须指定 --pg-url 或设置 DATABASE_URL 环境变量")

    results = migrate_all(
        sqlite_path=args.sqlite,
        pg_url=args.pg_url,
        tables=args.tables,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )

    failed = [t for t, v in results.items() if v < 0]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
