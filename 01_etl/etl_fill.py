import logging
import os
from datetime import datetime as dt

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from decorators import backoff
from loaders import EsDb, PostgreDb
from states import FileStorage, State


def postgres_to_es(connection: _connection, path: str):
    page_size = PAGE_SIZE if 'PAGE_SIZE' in globals() else 1000
    start_datetime_key = 'saved_dt'
    offset_key = 'saved_rows'
    storage = FileStorage()
    state = State(storage)
    start_datetime = dt.fromisoformat(state.get_state(start_datetime_key, dt.min.isoformat()))
    logging.debug('Start dt set to %s', start_datetime)
    offset = state.get_state(offset_key, 0)
    query_time = dt.utcnow()
    pg_getter = PostgreDb(connection=connection, page_size=page_size, start_datetime=start_datetime, offset=offset)
    es_setter = EsDb(path)
    for data in pg_getter.read_data():
        logging.debug('Start transfer data with offset=%s', offset)
        es_setter.bulk_write(data)
        offset += len(data)
        state.set_state(offset_key, offset)
        logging.debug('State offset=%s saved', offset)
    state.set_state(start_datetime_key, query_time.isoformat())
    state.clear_state(offset_key)
    logging.debug('Transfer complete.')


@backoff
def establish_connection(dsl: dict, es_path: str):
    with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        postgres_to_es(pg_conn, es_path)


if __name__ == '__main__':
    PAGE_SIZE: int = 100
    logging.basicConfig(level=logging.DEBUG)
    load_dotenv(override=True)
    dsl = {
        'dbname': os.getenv('DBNAME'),
        'user': os.getenv('USER'),
        'password': os.getenv('PASSWORD'),
        'host': os.getenv('HOST'),
        'port': os.getenv('PORT'),
        'options': '-c search_path=content',
    }
    es_path = os.getenv('ES_PATH', '')
    establish_connection(dsl, es_path)
