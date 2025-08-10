from contextlib import contextmanager
from typing import Generator
import psycopg

@contextmanager
def connection(db_url: str) -> Generator[psycopg.Connection, None, None]:
    """
    Create a new database connection.

    Note: we use neon postgres which does connection pooling at the infra level,
    so we don't need to worry about connection pooling here.

    Args:
        db_url: The URL of the database to connect to.

    Returns:
        A database connection.
    """
    conn = psycopg.connect(db_url)
    try:
        yield conn
    finally:
        conn.close()