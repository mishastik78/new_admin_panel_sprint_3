import logging
from datetime import datetime as dt

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from psycopg2.extensions import connection as _connection

from models import MoviesForEs


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
        WHERE fw.modified > %(sdate)s OR
              gfw.created > %(sdate)s OR
              g.modified > %(sdate)s OR
              pfw.created > %(sdate)s OR
              p.modified > %(sdate)s
        GROUP BY fw.id
        ORDER BY fw.modified;'''

    def __init__(self, *, connection: _connection, page_size: int, start_datetime: dt, offset: int = 0):
        logging.debug('Starting init cursor with offset=%s', offset)
        self.cursor = connection.cursor(name='cur', scrollable=True)
        self.cursor.arraysize = page_size
        self.cursor.execute(self.query, {'sdate': start_datetime})
        self.cursor.scroll(offset)
        logging.debug('Cursor ready')

    def read_data(self):
        logging.debug('Start reading from postgre')
        attrs = MoviesForEs.__fields__.keys()
        while data := self.cursor.fetchmany():
            logging.debug('Data exists')
            yield [MoviesForEs(**{key: row[key] for key in attrs}) for row in data]  # type: ignore
        logging.debug('Data empty')


class EsDb:
    settings = {
        "settings": {
            "refresh_interval": "1s",
            "analysis": {
                "filter": {
                    "english_stop": {
                        "type": "stop",
                        "stopwords": "_english_"
                    },
                    "english_stemmer": {
                        "type": "stemmer",
                        "language": "english"
                    },
                    "english_possessive_stemmer": {
                        "type": "stemmer",
                        "language": "possessive_english"
                    },
                    "russian_stop": {
                        "type": "stop",
                        "stopwords": "_russian_"
                    },
                    "russian_stemmer": {
                        "type": "stemmer",
                        "language": "russian"
                    }
                },
                "analyzer": {
                    "ru_en": {
                        "tokenizer": "standard",
                        "filter": [
                            "lowercase",
                            "english_stop",
                            "english_stemmer",
                            "english_possessive_stemmer",
                            "russian_stop",
                            "russian_stemmer"
                        ]
                    }
                }
            }
        }
    }
    mappings = {
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "imdb_rating": {
                    "type": "float"
                },
                "genre": {
                    "type": "keyword"
                },
                "title": {
                    "type": "text",
                    "analyzer": "ru_en",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                "description": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "director": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "writers_names": {
                    "type": "text",
                    "analyzer": "ru_en"
                },
                "actors": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                },
                "writers": {
                    "type": "nested",
                    "dynamic": "strict",
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text",
                            "analyzer": "ru_en"
                        }
                    }
                }
            }
        }
    }

    def __init__(self, path: str):
        self.es = Elasticsearch(path)
        if not self.es.indices.exists(index='movies'):
            self.es.indices.create(index='movies', mappings=self.mappings, settings=self.settings)

    def build_index(self, data: list[MoviesForEs]) -> list[dict]:
        return [{"_index": "movies", "_source": row.json()} for row in data]

    def bulk_write(self, data: list[MoviesForEs]) -> None:
        _, errors = bulk(self.es, self.build_index(data))
        if errors:
            logging.error('Errors while parsing bulk', errors)
