from piracyshield_component.environment import Environment
from piracyshield_component.log.logger import Logger

import os

# TODO: use custom exceptions.

class CacheStorage:

    logger = None

    def __init__(self):
        self.logger = Logger('storage')

        if not os.path.exists(Environment.CACHE_PATH):
            raise FileNotFoundError(f'The specified folder `{Environment.CACHE_PATH}` does not exist')

        if not os.path.isdir(Environment.CACHE_PATH):
            raise NotADirectoryError(f'The specified path `{Environment.CACHE_PATH}` is not a directory')

    def write(self, filename: str, content: bytes) -> str | Exception:
        path = self._get_absolute_path(filename)

        try:
            with open(path, 'wb') as handle:
                handle.write(content)

        except IOError:
            raise IOError(f'Failed to write content to file `{filename}`')

        # return the absolute path
        return path

    def get(self, filename: str) -> str | Exception:
        return self._get_absolute_path(filename)

    def read(self, filename: str) -> bytes | Exception:
        path = self._get_absolute_path(filename)

        try:
            with open(path, 'r') as handle:
                return handle.read()

        except FileNotFoundError:
            raise FileNotFoundError(f'The specified file `{filename}` does not exist')

        except IOError:
            raise IOError(f'Failed to read content from file `{filename}`')

    def exists(self, filename: str) -> bool:
        path = self._get_absolute_path(filename)

        return os.path.exists(path)

    def get_all(self) -> list | Exception:
        try:
            return os.listdir(Environment.CACHE_PATH)

        except IOError:
            raise IOError(f'Failed to get files from folder `{Environment.CACHE_PATH}`')

    def remove(self, filename: str) -> bool | Exception:
        path = self._get_absolute_path(filename)

        try:
            os.remove(path)

        except FileNotFoundError:
            raise FileNotFoundError(f'The specified file `{filename}` does not exist')

        except IOError:
            raise IOError(f'Failed to remove file `{filename}`.')

        return True

    def _get_absolute_path(self, filename: str) -> str:
        return os.path.join(Environment.CACHE_PATH, filename)
