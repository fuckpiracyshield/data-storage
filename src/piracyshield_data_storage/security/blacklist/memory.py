from piracyshield_data_storage.database.redis.document import DatabaseRedisDocument, DatabaseRedisSetException, DatabaseRedisGetException

class SecurityBlacklistMemory(DatabaseRedisDocument):

    ip_address_prefix = 'ip_address'

    token_prefix = 'token'

    def __init__(self, database: int):
        super().__init__()

        self.establish(database)

    def add_ip_address(self, ip_address: str, duration: int = 60) -> bool | Exception:
        """
        Blacklists an IP address.

        :param ip_address: a valid IP address.
        :param duration: duration of the blacklist in seconds.
        :return: true if the item has been stored.
        """

        try:
            return self.set_with_expiry(
                key = f'{self.ip_address_prefix}:{ip_address}',
                value = '1',
                expiry = duration
            )

        except DatabaseRedisSetException:
            raise SecurityBlacklistMemorySetException()

    def exists_by_ip_address(self, ip_address: str) -> bool | Exception:
        """
        Verifies if an IP address is in the blacklist.

        :param item: a valid IP address.
        :return: returns the TTL of the item.
        """

        try:
            response = self.get(key = f'{self.ip_address_prefix}:{ip_address}')

            if response:
                return True

            return False

        except DatabaseRedisGetException:
            raise SecurityBlacklistMemoryGetException()

    def remove_ip_address(self, ip_address: str) -> bool | Exception:
        """
        Removes an IP address from the blacklist.

        :param item: a valid IP address.
        :return: true if the item has been removed.
        """

        try:
            return self.delete(
                key = f'{self.ip_address_prefix}:{ip_address}'
            )

        except DatabaseRedisSetException:
            raise SecurityBlacklistMemorySetException()

class SecurityBlacklistMemorySetException(Exception):

    """
    Cannot set the value.
    """

    pass

class SecurityBlacklistMemoryGetException(Exception):

    """
    Cannot get the value.
    """

    pass
