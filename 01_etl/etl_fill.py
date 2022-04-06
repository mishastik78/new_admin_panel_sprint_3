
import json
import logging
import os
import time
from typing import Any, Optional

import psycopg2
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from pydantic import BaseModel


class FileStorage:
    def __init__(self, file_path: Optional[str] = None):
        self.file_path: str = file_path or 'state.json'

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as fp:
            json.dump(state, fp)

    def retrieve_state(self) -> dict:
        try:
            with open(self.file_path) as fp:
                state = json.load(fp)
        except FileNotFoundError:
            return {}
        return state


class State:
    def __init__(self, storage: FileStorage):
        self.storage = storage
        self.state: dict[str, Any] = storage.retrieve_state()

    def set_state(self, key: str, value) -> None:
        self.state.update({key: value})
        self.storage.save_state(self.state)

    def get_state(self, key: str, default: Optional[Any] = None) -> Any:
        return self.state.get(key, default)

    def clear_state(self, key: str) -> None:
        self.state.pop(key, None)
        self.storage.save_state(self.state)


class EsData(BaseModel):
    id: str
    title: str
    description: str | None
    imdb_rating: float | None
    genre: str | None
    director: str | None
    actors_names: str | None
    writers_names: str | None
    actors: list[dict[str, str]] | None
    writers: list[dict[str, str]] | None


class PostgreDb:
    query = '''SELECT fw.id, fw.title, fw.description, fw.rating AS imdb_rating,
        STRING_AGG(DISTINCT g.name, ', ') AS genre,
        STRING_AGG(DISTINCT p.full_name, ', ') FILTER (WHERE pfw.role = 'director') AS director,
        STRING_AGG(DISTINCT p.full_name, ', ') FILTER (WHERE pfw.role = 'actor') AS actors_names,
        STRING_AGG(DISTINCT p.full_name, ', ') FILTER (WHERE pfw.role = 'writer') AS writers_names,
        JSON_AGG(json_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'actor') AS actors,
        JSON_AGG(json_build_object('id', p.id, 'name', p.full_name)) FILTER (WHERE pfw.role = 'writer') AS writers
        FROM film_work fw
        LEFT OUTER JOIN genre_film_work gfw ON fw.id = gfw.film_work_id
        LEFT OUTER JOIN genre g ON gfw.genre_id = g.id
        LEFT OUTER JOIN person_film_work pfw ON fw.id = pfw.film_work_id
        LEFT OUTER JOIN person p ON pfw.person_id = p.id
        GROUP BY fw.id
        ORDER BY fw.created;'''

    def __init__(self, connection: _connection, page_size: int, offset: int = 0):
        logging.debug(f'Starting init cursor with {offset=}')
        self.cursor = connection.cursor(name='cur', scrollable=True)
        self.cursor.arraysize = page_size
        self.cursor.execute(self.query)
        self.cursor.scroll(offset)
        logging.debug('Cursor ready')

    def read_data(self):
        logging.debug('Start reading from postgre')
        attrs = EsData.__fields__.keys()
        while data := self.cursor.fetchmany():
            logging.debug('Data exists')
            yield [EsData(**{key: row[key] for key in attrs}) for row in data]  # type: ignore
        logging.debug('Data empty')


class EsDb:
    def __init__(self, path: str):
        self.es = Elasticsearch(path)

    def bulk_write(self, data: list[EsData]) -> None:
        rows = [{"_index": "movies", "_source": row.json()} for row in data]
        result = bulk(self.es, rows)
        if result[1]:
            logging.error('Errors while parsing bulk', result[1])


def postgres_to_es(connection: _connection, path: str, page_size: int):
    key = 'saved_to_es'
    storage = FileStorage()
    state = State(storage)
    offset = state.get_state(key, 0)
    pg_getter = PostgreDb(connection, page_size, offset)
    es_setter = EsDb(path)
    for data in pg_getter.read_data():
        logging.debug(f'Start transfer data with {offset=}')
        es_setter.bulk_write(data)
        offset += len(data)
        state.set_state(key, offset)
        logging.debug(f'State {offset=} saved')
    state.clear_state(key)
    logging.debug('Transfer complete.')


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
    es_path = 'http://127.0.0.1:9200'
    max_retries: int = 10
    initial_backoff: float = 2
    max_backoff: float = 600
    for attempt in range(max_retries + 1):
        if attempt:
            logging.debug(f'Connection {attempt=}')
            time.sleep(min(max_backoff, initial_backoff * 2 ** (attempt - 1)))
        try:
            with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
                postgres_to_es(pg_conn, es_path, PAGE_SIZE)
        except psycopg2.Error as e:
            logging.exception(e)
            if attempt == max_retries:
                raise
        else:
            break
