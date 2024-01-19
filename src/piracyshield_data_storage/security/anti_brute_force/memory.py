from piracyshield_data_storage.database.redis.document import DatabaseRedisDocument, DatabaseRedisSetException, DatabaseRedisGetException

class SecurityAntiBruteForceMemory(DatabaseRedisDocument):

    def __init__(self, database: int):
        super().__init__()

        self.establish(database)

    def set_login_attempts(self, email: str, timeframe: int, attempts: int = 1) -> bool | Exception:
        """
        Sets the current login attempts in a given timeframe.

        :param email: a valid e-mail address.
        :param timeframe: a timeframe in seconds.
        :param attempts: sets number of attempts, default: 1.
        :return: true if the value has been stored.
        """

        try:
            return self.set_with_expiry(
                key = email,
                value = attempts, # store the correct types
                expiry = timeframe
            )

        except DatabaseRedisSetException:
            raise SecurityMemorySetException()

    def increment_login_attempts(self, email: str, amount: int = 1) -> bool | Exception:
        """
        Increments the attempts by preserving expiry time.

        :param email: a valid e-mail address.
        :param amount: increment by a number, default: 1.
        :return: true if successfully executed.
        """

        try:
            return self.incr(
                key = email,
                amount = amount
            )

        except DatabaseRedisSetException:
            raise SecurityMemorySetException()

    def get_login_attempts(self, email: str) -> int | Exception:
        """
        Gets the current account login attempts.

        :param email: a valid e-mail address.
        :return: the number of attempts.
        """

        try:
            response = self.get(key = email)

            if response:
                # make sure we're dealing with a true int and not a string
                return int(response)

            return response

        except DatabaseRedisGetException:
            raise SecurityMemoryGetException()

    def reset_login_attempts(self, email: str) -> bool | Exception:
        """
        Unsets the login attempts count.

        :param email: a valid e-mail address.
        :return: true if successfully executed.
        """

        try:
            return self.delete(
                key = email
            )

        except DatabaseRedisSetException:
            raise SecurityMemorySetException()

class SecurityAntiBruteForceMemorySetException(Exception):

    """
    Cannot set the value.
    """

    pass

class SecurityAntiBruteForceMemoryGetException(Exception):

    """
    Cannot get the value.
    """

    pass
