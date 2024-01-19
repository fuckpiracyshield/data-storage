from piracyshield_component.config import Config

from redis import Redis

class DatabaseRedisConnection:

    instance = None

    database_config = None

    def __init__(self) -> None:
        self._prepare_configs()

    def establish(self, database: str) -> None:
        try:
            self.instance = Redis(
                host = self.database_config['host'],
                port = self.database_config['port'],
                db = database,
                decode_responses = True
            )

        except:
            raise DatabaseRedisConnectionException()

    def _prepare_configs(self) -> None:
        self.database_config = Config('database/redis').get('connection')

class DatabaseRedisConnectionException(Exception):

    pass
