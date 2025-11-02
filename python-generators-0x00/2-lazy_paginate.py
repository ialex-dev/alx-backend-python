#!/usr/bin/python3
"""
2-lazy_paginate.py

Objective:
Simulate fetching paginated data from the users database lazily
using a generator.

Functions:
- paginate_users(page_size, offset)
- lazy_pagination(page_size)
"""

import seed


def paginate_users(page_size, offset):
    """
    Fetch a single page of users from the user_data table.
    Returns a list of rows (dicts).
    """
    connection = seed.connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM user_data LIMIT {page_size} OFFSET {offset}")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()
    return rows


def lazy_pagination(page_size):
    """
    Generator that lazily fetches pages of users one by one
    using only ONE loop.

    Yields:
        A list of user rows (each page) from user_data.
    """
    offset = 0
    while True:  # single loop
        page = paginate_users(page_size, offset)
        if not page:
            break  # stop when no more records
        yield page
        offset += page_size
