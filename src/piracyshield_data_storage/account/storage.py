from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class AccountStorage(DatabaseArangodbDocument):

    collection_name = None

    collection_instance = None

    def __init__(self, collection_name: str):
        super().__init__()

        self.collection_name = collection_name

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict | Exception:
        try:
            return self.collection_instance.insert(document)

        except:
            raise AccountStorageCreateException()

    def get(self, account_id: str) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.collection_name}
            FILTER document.account_id == @account_id
            RETURN {{
                'account_id': document.account_id,
                'name': document.name,
                'email': document.email,
                'role': document.role,
                'is_active': document.is_active,
                'created_by': document.created_by
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'account_id': account_id
            })

        except:
            raise AccountStorageGetException()

    def get_complete(self, account_id: str) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.collection_name}

                FILTER document.account_id == @account_id

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'account_id': account_id
            })

        except:
            raise AccountStorageGetException()

    def get_all(self) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.collection_name}
            RETURN {{
                'account_id': document.account_id,
                'name': document.name,
                'email': document.email,
                'is_active': document.is_active,
                'role': document.role
            }}
        """

        try:
            return self.query(aql)

        except:
            raise AccountStorageGetException()

    def get_total(self) -> int | Exception:
        try:
            return self.collection_instance.count()

        except:
            raise AccountStorageGetException()

    def get_active(self) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.is_active == true

            RETURN {{
                'account_id': document.account_id,
                'name': document.name,
                'email': document.email,
                'role': document.role
            }}
        """

        try:
            return self.query(aql)

        except:
            raise AccountStorageGetException()

    def get_total(self) -> int | Exception:
        try:
            return self.collection_instance.count()

        except:
            raise AccountStorageGetException()

    def exists_by_identifier(self, identifier: str) -> Cursor | Exception:
        """
        Checks if an account with this identifier is in the collection.

        :param value: a valid account identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.account_id == @identifier

            LIMIT 1

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'identifier': identifier
            })

        except:
            raise AccountStorageGetException()

    def set_flag(self, account_id: str, flag: str, value: any) -> Cursor | Exception:
        """
        Sets a flag with a status.

        :param account_id: account identifier.
        :param flag: the flag to update.
        :param value: any value.
        :return: the number of updated rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}

                FILTER document.account_id == @account_id

            UPDATE {{
                '_key': document._key,
                'flags': {{
                    @flag: @value
                }}
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'account_id': account_id,
                    'flag': flag,
                    'value': value
                },
                count = True
            )

            return affected_rows

        except:
            raise AccountStorageUpdateException()

    def change_password(self, account_id: str, password: str) -> Cursor | Exception:
        """
        Changes the accounts' password.

        :param account_id: account identifier.
        :param password: the encoded password.
        :return: the number of updated rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}

                FILTER document.account_id == @account_id

            UPDATE {{
                '_key': document._key,
                'password': @password
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'account_id': account_id,
                    'password': password
                },
                count = True
            )

            return affected_rows

        except:
            raise AccountStorageUpdateException()

    def update_status(self, account_id: str, value: bool) -> Cursor | Exception:
        """
        Updates the account status.

        :param account_id: account identifier.
        :param is_active: account status.
        :return: the number of updated rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}

                FILTER document.account_id == @account_id

            UPDATE {{
                '_key': document._key,
                'is_active': @value
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'account_id': account_id,
                    'value': value
                },
                count = True
            )

            return affected_rows

        except:
            raise AccountStorageUpdateException()

    def remove(self, account_id: str) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.account_id == @account_id
            REMOVE document IN {self.collection_name}

            RETURN OLD
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'account_id': account_id
                },
                count = True
            )

            return affected_rows

        except:
            raise AccountStorageRemoveException()

class AccountStorageCreateException(Exception):

    """
    Cannot create the account.
    """

    pass

class AccountStorageGetException(Exception):

    """
    Cannot get the account.
    """

    pass

class AccountStorageUpdateException(Exception):

    """
    Cannot update the account.
    """

    pass

class AccountStorageRemoveException(Exception):

    """
    Cannot remove the account.
    """

    pass
