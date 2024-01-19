from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class AuthenticationStorage(DatabaseArangodbDocument):

    view_name = 'accounts_view'

    def __init__(self):
        super().__init__()

    def get(self, email: str) -> Cursor | Exception:
        """
        Returns the account via email.

        :param email: a valid e-mail address.
        :return: cursor with the requested data.
        """

        try:
            aql = f"""
                FOR document IN {self.view_name}

                FILTER document.email == @email

                RETURN document
            """

            return self.query(aql, bind_vars = {
                'email': email
            })

        except:
            raise AuthenticationStorageGetException()

class AuthenticationStorageGetException(Exception):

    """
    Cannot get the account email.
    """

    pass
