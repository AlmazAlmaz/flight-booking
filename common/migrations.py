"""Apply each numbered SQL file once, using a PostgreSQL transaction."""

from pathlib import Path

import psycopg


def migrate(database_url: str, directory: Path) -> None:
    migrations = sorted(directory.glob("[0-9]*.sql"))
    if not migrations:
        raise RuntimeError(f"No SQL migrations found in {directory}")

    with psycopg.connect(database_url) as conn:
        # Prevent simultaneous service starts from applying the same migration.
        conn.execute("SELECT pg_advisory_xact_lock(601203)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        applied = {row[0] for row in conn.execute("SELECT version FROM schema_migrations")}
        for migration in migrations:
            if migration.name in applied:
                continue
            conn.execute(migration.read_text(encoding="utf-8"))
            conn.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s)",
                (migration.name,),
            )
