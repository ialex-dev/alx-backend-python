#!/usr/bin/python3
"""
4-stream_ages.py

Objective:
Use a generator to compute a memory-efficient average age for a large dataset.

Functions:
- stream_user_ages(): yields ages one by one from the database
- compute_average_age(): calculates average age using the generator
"""

import seed


def stream_user_ages():
    """
    Generator that yields user ages one by one from the database.
    """
    connection = seed.connect_to_prodev()
    cursor = connection.cursor()
    cursor.execute("SELECT age FROM user_data")

    for (age,) in cursor:  # loop 1
        yield int(age)

    cursor.close()
    connection.close()


def compute_average_age():
    """
    Computes the average age using the stream_user_ages generator,
    without loading the full dataset into memory.
    """
    total_age = 0
    count = 0

    for age in stream_user_ages():  # loop 2
        total_age += age
        count += 1

    if count == 0:
        print("No user data found.")
    else:
        avg_age = total_age / count
        print(f"Average age of users: {avg_age:.2f}")


if __name__ == "__main__":
    compute_average_age()
