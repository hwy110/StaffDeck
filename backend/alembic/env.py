"""Alembic 迁移环境配置 — StaffDeck

此文件在每次运行 alembic 命令时执行，负责：
1. 从环境变量 / .env 读取 DATABASE_URL
2. 导入所有 SQLModel 模型以构建完整 metadata
3. 配置目标数据库连接
"""

from __future__ import annotations

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

# 将 backend/ 加入 Python 路径，确保 app 模块可导入
_backend_dir = Path(__file__).resolve().parents[1]
if str(_backend_dir) not in sys.path:
    sys.path.insert(0, str(_backend_dir))

# ── 导入所有模型（必须在 metadata 使用之前完成）──────────────────────
import app.db.models  # noqa: F401,E402

# ── Alembic Config 对象 ──────────────────────────────────────────────
config = context.config

# 日志配置
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ── 数据库 URL：优先从环境变量读取，其次从 alembic.ini ──────────────
def _resolve_database_url() -> str:
    """从多个来源解析 DATABASE_URL。"""
    # 1. 环境变量（最高优先级）
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    # 2. .env 文件
    dotenv_path = _backend_dir / ".env"
    if dotenv_path.exists():
        for line in dotenv_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("DATABASE_URL="):
                raw = line.split("=", 1)[1].strip().strip("\"'")
                if raw:
                    return raw

    # 3. alembic.ini 中的 sqlalchemy.url
    url = config.get_main_option("sqlalchemy.url", "")
    if url:
        return url

    raise RuntimeError(
        "未找到 DATABASE_URL 配置。请设置环境变量或在 .env / alembic.ini 中配置。"
    )


database_url = _resolve_database_url()
config.set_main_option("sqlalchemy.url", database_url)

# ── 目标 metadata ────────────────────────────────────────────────────
target_metadata = SQLModel.metadata


def run_migrations_offline() -> None:
    """离线模式：仅生成 SQL 脚本，不连接数据库。"""
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """在线模式：连接数据库执行迁移。"""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
