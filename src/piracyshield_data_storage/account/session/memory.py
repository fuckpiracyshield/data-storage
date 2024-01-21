from piracyshield_data_storage.database.redis.document import DatabaseRedisDocument, DatabaseRedisSetException, DatabaseRedisGetException

class AccountSessionMemory(DatabaseRedisDocument):

    session_prefix = 'session'

    def __init__(self, database: int):
        super().__init__()

        self.establish(database)

    def add_long_session(self, account_id: str, refresh_token: str, data: dict, duration: int) -> bool | Exception:
        """
        Store an access token generated from a refresh token.

        :param refresh_token: a valid refresh token.
        :param access_token: a valid access token.
        :param duration: refresh token expire time.
        :return: true if the item has been stored.
        """

        try:
            return self.hset_with_expiry(
                key = f'{self.session_prefix}:{account_id}:long:{refresh_token}',
                mapping = data,
                expiry = duration
            )

        except DatabaseRedisSetException:
            raise AccountSessionMemorySetException()

    def add_short_session(self, account_id: str, refresh_token: str, access_token: str, data: dict, duration: int) -> bool | Exception:
        """
        Store an access token generated from a refresh token.

        :param refresh_token: a valid refresh token.
        :param access_token: a valid access token.
        :param duration: refresh token expire time.
        :return: true if the item has been stored.
        """

        try:
            return self.hset_with_expiry(
                key = f'{self.session_prefix}:{account_id}:short:{access_token}',
                mapping = data,
                expiry = duration
            )

        except DatabaseRedisSetException:
            raise AccountSessionMemorySetException()

    def get_all_by_account(self, account_id: str) -> list | Exception:
        """
        Retrieves all the active long and short sessions.

        :param account_id: a valid account identifier.
        :return: the requested data.
        """

        try:
            return self.keys(
                key = f'{self.session_prefix}:{account_id}:*:*'
            )

        except DatabaseRedisGetException:
            raise AccountSessionMemoryGetException()

    def get_all_short_by_account(self, account_id: str) -> list | Exception:
        """
        Retrieves all the active short sessions.

        :param account_id: a valid account identifier.
        :return: the requested data.
        """

        try:
            return self.keys(
                key = f'{self.session_prefix}:{account_id}:short:*'
            )

        except DatabaseRedisGetException:
            raise AccountSessionMemoryGetException()

    def get_session(self, session: str) -> list | Exception:
        """
        Retrieves a single session.

        :param token: a valid refresh or access identifier.
        :return: the requested data.
        """

        try:
            return self.hgetall(
                key = session
            )

        except DatabaseRedisGetException:
            raise AccountSessionMemoryGetException()

    def find_long_session(self, refresh_token: str) -> list | Exception:
        """
        Retrieves a single session.

        :param token: a valid refresh or access identifier.
        :return: the requested data.
        """

        try:
            response = self.keys(
                key = f'{self.session_prefix}:*:long:{refresh_token}'
            )

            if isinstance(response, list) and len(response):
                return response.__getitem__(0)

            return response

        except DatabaseRedisGetException:
            raise AccountSessionMemoryGetException()

    def remove_long_session(self, account_id: str, refresh_token: str) -> bool | Exception:
        """
        Removes a long session.

        :param account_id: a valid account identifier.
        :param refresh_token: a valid active refresh token.
        :return: true if the item has been removed.
        """

        try:
            return self.delete(
                key = f'{self.session_prefix}:{account_id}:long:{refresh_token}'
            )

        except DatabaseRedisSetException:
            raise AccountSessionMemorySetException()

    def remove_short_session(self, account_id: str, access_token: str) -> bool | Exception:
        """
        Removes a short session.

        :param account_id: a valid account identifier.
        :param access_token: a valid active access token.
        :return: true if the item has been removed.
        """

        try:
            return self.delete(
                key = f'{self.session_prefix}:{account_id}:short:{access_token}'
            )

        except DatabaseRedisSetException:
            raise AccountSessionMemorySetException()

class AccountSessionMemorySetException(Exception):

    """
    Cannot set the value.
    """

    pass

class AccountSessionMemoryGetException(Exception):

    """
    Cannot get the value.
    """

    pass
