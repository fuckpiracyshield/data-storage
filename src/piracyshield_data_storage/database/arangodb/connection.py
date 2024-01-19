from piracyshield_component.config import Config

from arango import ArangoClient

class DatabaseArangodbConnection:

    instance = None

    database_config = None

    def __init__(self, as_root = False):
        self._prepare_configs()

        self._prepare_settings()

        self._prepare_credentials(as_root)

        self.establish()

    def establish(self):
        client = ArangoClient(hosts = f'{self.protocol}://{self.host}:{self.port}')

        self.instance = client.db(self.database, self.username, self.password, self.verify)

    def _prepare_settings(self):
        connection = self.database_config.get('connection')

        try:
            self.protocol = connection['protocol']

            self.host = connection['host']

            self.port = connection['port']

            self.verify = connection['verify']

        except KeyError:
            DatabaseArangodbConnectionException('Cannot find the database settings')

        try:
            self.database = connection['database']

        except KeyError:
            DatabaseArangodbConnectionException('Cannot find the database name')

    def _prepare_credentials(self, as_root):
        credentials = self.database_config.get('root_credentials') if as_root else self.database_config.get('user_credentials')

        try:
            self.username = credentials['username']

            self.password = credentials['password']

        except KeyError:
            DatabaseArangodbConnectionException('Cannot find the database credentials')

    def _prepare_configs(self):
        self.database_config = Config('database/arangodb')

class DatabaseArangodbConnectionException(Exception):

    pass
