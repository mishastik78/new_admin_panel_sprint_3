import logging
import time

import psycopg2


def backoff(func):
    def wrapper(*args, **kwargs):
        max_retries: int = 10
        initial_backoff: float = 2
        max_backoff: float = 600
        for attempt in range(max_retries + 1):
            if attempt:
                logging.debug('Connection attempt %s', attempt)
                time.sleep(min(max_backoff, initial_backoff * 2 ** (attempt - 1)))
            try:
                func(*args, **kwargs)
            except psycopg2.Error as e:
                logging.exception(e)
                if attempt == max_retries:
                    raise
            else:
                break
    return wrapper
