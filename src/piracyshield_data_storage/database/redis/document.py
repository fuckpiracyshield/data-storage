from piracyshield_data_storage.database.redis.connection import DatabaseRedisConnection

class DatabaseRedisDocument(DatabaseRedisConnection):

    def set_with_expiry(self, key: str, value: any, expiry: int) -> bool | Exception:
        if self.instance.set(key, value, ex = expiry) == True:
            return True

        raise DatabaseRedisSetException()

    def incr(self, key: str, amount: int = 1) -> bool | Exception:
        return self.instance.incr(name = key, amount = amount)

    def get(self, key: str) -> any:
        return self.instance.get(key)

    def delete(self, key: str) -> any:
        return self.instance.delete(key)

class DatabaseRedisSetException(Exception):

    """
    Cannot set the data.
    """

    pass

class DatabaseRedisGetException(Exception):

    """
    Cannot get the data.
    """

    pass
