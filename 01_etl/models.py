from pydantic import BaseModel


class MoviesForEs(BaseModel):
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
