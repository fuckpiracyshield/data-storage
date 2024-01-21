from piracyshield_data_storage.database.redis.connection import DatabaseRedisConnection

class DatabaseRedisDocument(DatabaseRedisConnection):

    def keys(self, key: str) -> any:
        try:
            return self.instance.keys(
                name = key
            )

        except:
            raise DatabaseRedisGetException()

    # string

    def set_with_expiry(self, key: str, value: any, expiry: int) -> bool | Exception:
        if self.instance.set(key, value, ex = expiry) == True:
            return True

        raise DatabaseRedisSetException()

    def setnx_with_expiry(self, key: str, value: any, expiry: int) -> bool | Exception:
        pipeline = self.instance.pipeline()

        pipeline.setnx(
            name = key,
            value = value
        )

        pipeline.expire(
            name = key,
            time = expiry
        )

        result = pipeline.execute()

        if result:
            return True

        raise DatabaseRedisSetException()

    def incr(self, key: str, amount: int = 1) -> bool | Exception:
        return self.instance.incr(name = key, amount = amount)

    def get(self, key: str) -> any:
        return self.instance.get(key)

    def delete(self, key: str) -> any:
        return self.instance.delete(key)

    # hash

    def hset_with_expiry(self, key: str, mapping: list, expiry: int) -> bool | Exception:
        pipeline = self.instance.pipeline()

        pipeline.hset(
            name = key,
            mapping = mapping
        )

        pipeline.expire(
            name = key,
            time = expiry
        )

        result = pipeline.execute()

        if result:
            return True

        raise DatabaseRedisSetException()

    def hgetall(self, key: str) -> any:
        try:
            return self.instance.hgetall(
                name = key
            )

        except:
            raise DatabaseRedisGetException()

    # list

    def lpush_with_expiry(self, key: str, value: str, expiry: int) -> bool | Exception:
        pipeline = self.instance.pipeline()

        pipeline.lpush(
            key,
            value
        )

        pipeline.expire(
            name = key,
            time = expiry
        )

        result = pipeline.execute()

        if result:
            return True

        raise DatabaseRedisSetException()

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
