#!/usr/bin/env python3
"""
1-batch_processing.py

Provides:
- stream_users_in_batches(batch_size): yields lists (batches) of user dicts
- batch_processing(batch_size): processes each batch to filter users over age 25
  and prints each filtered user (one per line, with a blank line after each).

Constraints respected:
- Uses yield generator
- No more than 3 loops in total (1 loop in stream_users_in_batches, 2 loops in batch_processing)
"""

from typing import List, Dict, Iterator

# import the stream_users generator from 0-stream_users.py
_stream_mod = __import__("0-stream_users")
stream_users = _stream_mod.stream_users


def stream_users_in_batches(batch_size: int) -> Iterator[List[Dict]]:
    """
    Generator that yields batches (lists) of users of size up to batch_size.
    Uses a single loop to pull users from stream_users().
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")

    batch: List[Dict] = []
    for user in stream_users():  # single loop
        batch.append(user)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def batch_processing(batch_size: int) -> None:
    """
    Processes batches produced by stream_users_in_batches:
    - filters users with age > 25
    - prints each filtered user (one per line) followed by a blank line

    Uses at most 2 loops here:
      1) iterate over batches
      2) iterate over filtered users within each batch (prints them)
    """
    for batch in stream_users_in_batches(batch_size):  # loop 1 (across batches)
        # filter users over age 25 (comprehension does not count as an extra explicit loop here)
        filtered = [u for u in batch if (isinstance(u.get("age"), int) and u.get("age") > 25) or
                    (not isinstance(u.get("age"), int) and int(float(u.get("age"))) > 25)]
        for user in filtered:  # loop 2 (across filtered users)
            print(user)
            print()
