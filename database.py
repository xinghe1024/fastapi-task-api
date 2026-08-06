from sqlite3 import Connection as SQLiteConnection
from typing import Any
from config import get_settings

from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker

settings = get_settings()

DATABASE_URL = settings.database_url

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False
    },
)

SessionFactory = sessionmaker(
    bind=engine,
)

class Base(DeclarativeBase):
    pass

def enable_sqlite_foreign_keys(
    database_connection: SQLiteConnection,
    _connection_record: Any,
) -> None:
    previous_autocommit = database_connection.autocommit
    database_connection.autocommit = True

    cursor = database_connection.cursor()

    try:
        cursor.execute("PRAGMA foreign_keys=ON")
    finally:
        cursor.close()
        database_connection.autocommit = (
            previous_autocommit
        )

event.listen(
    engine,
    "connect",
    enable_sqlite_foreign_keys,
)