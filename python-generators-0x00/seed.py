#!/usr/bin/env python3
"""
seed.py

Uses local `user_data.csv` (in project root) to seed the ALX_prodev.user_data table
and provides a generator to stream rows one-by-one.

Prototypes implemented:
- connect_db()
- create_database(connection)
- connect_to_prodev()
- create_table(connection)
- insert_data(connection, data)
- stream_user_data(connection)

"""

import os
import csv
import uuid
from typing import Iterator, Optional, Tuple
import mysql.connector
from mysql.connector import Error

# --- Configuration (use env vars; sensible defaults) ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")  # set via env var for security
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_NAME = "ALX_prodev"
TABLE_NAME = "user_data"
DEFAULT_CSV = "user_data.csv"  # local CSV file in your repo root


def connect_db() -> Optional[mysql.connector.connection_cext.CMySQLConnection]:
    """
    Connect to MySQL server
    """
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT,
            autocommit=False,
        )
        return conn
    except Error as e:
        print(f"[connect_db] Error connecting to MySQL server: {e}")
        return None


def create_database(connection: mysql.connector.connection_cext.CMySQLConnection) -> None:
    """
    Create the ALX_prodev database if it does not exist.
    """
    sql = f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        print(f"✅ Database `{DB_NAME}` created or already exists")
    except Error as e:
        print(f"[create_database] Error creating database: {e}")
    finally:
        if cursor:
            cursor.close()


def connect_to_prodev() -> Optional[mysql.connector.connection_cext.CMySQLConnection]:
    """
    Connect to the ALX_prodev database and return the connection (or None).
    """
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            port=MYSQL_PORT,
            database=DB_NAME,
            autocommit=False,
        )
        return conn
    except Error as e:
        print(f"[connect_to_prodev] Error connecting to DB `{DB_NAME}`: {e}")
        return None


def create_table(connection: mysql.connector.connection_cext.CMySQLConnection) -> None:
    """
    Create the user_data table if it does not exist.

    Columns:
      - user_id VARCHAR(36) PRIMARY KEY (UUID)
      - name VARCHAR(100) NOT NULL
      - email VARCHAR(100) NOT NULL UNIQUE
      - age DECIMAL(5,0) NOT NULL
    """
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        user_id VARCHAR(36) NOT NULL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(100) NOT NULL,
        age DECIMAL(5,0) NOT NULL,
        UNIQUE KEY uq_email (email),
        INDEX idx_user_id (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cursor = None
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        connection.commit()
        print(f"✅ Table `{TABLE_NAME}` created successfully (or already exists)")
    except Error as e:
        print(f"[create_table] Error creating table: {e}")
    finally:
        if cursor:
            cursor.close()


def insert_data(connection: mysql.connector.connection_cext.CMySQLConnection, data: str) -> None:
    """
    Insert rows from local CSV file `data` into the user_data table.
    - `data` should be a path to the CSV (e.g., 'user_data.csv').
    - CSV must have headers: name,email,age
    - Skips rows with duplicate email (UNIQUE constraint prevents duplicates).
    """
    if not os.path.exists(data):
        raise FileNotFoundError(f"[insert_data] CSV file not found: {data}")

    required_fields = {"name", "email", "age"}
    cursor = None
    try:
        with open(data, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not required_fields.issubset(set(reader.fieldnames or [])):
                raise ValueError(f"[insert_data] CSV missing required headers: {required_fields}")

            cursor = connection.cursor()
            inserted = 0
            for row in reader:
                name = (row.get("name") or "").strip()
                email = (row.get("email") or "").strip()
                age_raw = (row.get("age") or "").strip()

                if not email:
                    # skip rows without email
                    continue

                # Convert age to integer-like decimal; fallback to 0 if invalid
                try:
                    age_val = int(float(age_raw))
                except Exception:
                    age_val = 0

                user_id = str(uuid.uuid4())
                try:
                    cursor.execute(
                        f"INSERT INTO `{TABLE_NAME}` (user_id, name, email, age) VALUES (%s, %s, %s, %s);",
                        (user_id, name, email, age_val),
                    )
                    inserted += 1
                except mysql.connector.IntegrityError:
                    # Duplicate email or other integrity issue — skip
                    continue

            connection.commit()
            print(f"✅ Inserted {inserted} new rows from {data}")
    except Error as e:
        print(f"[insert_data] MySQL error: {e}")
    except Exception as e:
        print(f"[insert_data] Error: {e}")
    finally:
        if cursor:
            cursor.close()


def stream_user_data(connection: mysql.connector.connection_cext.CMySQLConnection) -> Iterator[Tuple[str, str, str, int]]:
    """
    Generator that yields one row at a time from user_data.
    Keep the connection open while iterating.
    Yields tuples: (user_id, name, email, age)
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT user_id, name, email, age FROM `{TABLE_NAME}`;")
        for row in cursor:
            yield row
    except Error as e:
        print(f"[stream_user_data] Error streaming data: {e}")
    finally:
        try:
            cursor.close()
        except Exception:
            pass


# Demonstration when run as script
if __name__ == "__main__":
    csv_path = DEFAULT_CSV

    if not MYSQL_PASSWORD:
        print("Warning: MYSQL_PASSWORD is empty. Consider exporting MYSQL_PASSWORD env var for safety.")

    conn = connect_db()
    if not conn:
        raise SystemExit("[main] Could not connect to MySQL server. Exiting.")

    create_database(conn)
    conn.close()

    conn = connect_to_prodev()
    if not conn:
        raise SystemExit(f"[main] Could not connect to DB {DB_NAME}. Exiting.")

    create_table(conn)

    try:
        insert_data(conn, csv_path)
    except Exception as e:
        print(e)
        conn.close()
        raise SystemExit("[main] Seeding failed. Exiting.")

    # quick checks
    try:
        cur = conn.cursor()
        cur.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s;", (DB_NAME,))
        if cur.fetchone():
            print(f"Database {DB_NAME} is present")
        cur.execute(f"SELECT * FROM `{TABLE_NAME}` LIMIT 5;")
        rows = cur.fetchall()
        print("First 5 rows:", rows)
        cur.close()
    except Error as e:
        print(f"[main] Check error: {e}")

    # demo streaming (first 3 rows)
    print("Streaming rows (first 3):")
    i = 0
    for r in stream_user_data(conn):
        print(r)
        i += 1
        if i >= 3:
            break

    conn.close()
