#!/usr/bin/env python3
"""
0-stream_users.py

Provides:
    def stream_users() -> generator that yields one user record at a time (as dict)
"""

import os
import mysql.connector
from mysql.connector import Error
from typing import Iterator, Dict

# DB config via environment variables (defaults kept for local dev)
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")  # set via env var
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_NAME = "ALX_prodev"
TABLE_NAME = "user_data"


def _connect_to_prodev():
    """Return a connection to the ALX_prodev database (or raise)."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT,
        database=DB_NAME,
    )


def stream_users() -> Iterator[Dict]:
    """
    Generator that yields rows from user_data one by one as dictionaries:
      {'user_id': ..., 'name': ..., 'email': ..., 'age': ...}

    IMPORTANT: keep the generator running while iterating to keep the DB connection open.
    Only one loop is used below (for row in cursor).
    """
    conn = None
    cursor = None
    try:
        conn = _connect_to_prodev()
        # Use dictionary=True so each cursor row is already a dict (no extra loop)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT user_id, name, email, age FROM `{TABLE_NAME}`;")
        # single loop that yields rows one-by-one
        for row in cursor:
            # Optionally normalize types (age to int)
            try:
                row["age"] = int(row["age"])
            except Exception:
                # if conversion fails, leave as-is
                pass
            yield row
    except Error as e:
        # If desired, raise or print; raising will propagate to caller
        raise
    finally:
        # Clean up resources when generator is exhausted or closed
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass
        try:
            if conn:
                conn.close()
        except Exception:
            pass
