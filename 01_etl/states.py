import json
from typing import Any, Optional


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
        except Exception:
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
