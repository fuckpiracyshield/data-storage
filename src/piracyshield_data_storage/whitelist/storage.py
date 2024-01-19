from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class WhitelistStorage(DatabaseArangodbDocument):

    collection_name = 'whitelist'

    collection_instance = None

    def __init__(self):
        super().__init__()

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict | Exception:
        """
        Adds a new whitelist item.

        :param document: dictionary with the expected data model values.
        :return: cursor with the inserted data.
        """

        try:
            return self.collection_instance.insert(document)

        except:
            raise WhitelistStorageCreateException()

    def get_all(self, account_id: str) -> Cursor | Exception:
        """
        Fetches all the whitelist items created by an account.

        :param account_id: a valid account identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.metadata.created_by == @account_id

            SORT document.value ASC

            RETURN {{
                'genre': document.genre,
                'value': document.value,
                'is_active': document.is_active,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'account_id': account_id
            })

        except:
            raise WhitelistStorageGetException()

    def get_global(self) -> Cursor | Exception:
        """
        Fetches the whitelist items of all the accounts.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            // resolve account identifier
            LET created_by_name = (
                FOR a IN accounts_view
                FILTER a.account_id == document.metadata.created_by
                RETURN a['name']
            )[0]

            SORT document.value ASC

            RETURN {{
                'genre': document.genre,
                'value': document.value,
                'is_active': document.is_active,
                'metadata': {{
                    'created_at': document.metadata.created_at,
                    'updated_at': document.metadata.updated_at,
                    'created_by': document.metadata.created_by,
                    'created_by_name': created_by_name
                }}
            }}
        """

        try:
            return self.query(aql)

        except:
            raise WhitelistStorageGetException()

    def exists_by_value(self, value: str) -> Cursor | Exception:
        """
        Searches for an item.

        :param value: item value.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.value == @value

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'value': value
            })

        except:
            raise WhitelistStorageGetException()

    def update_status(self, value: str, status: bool) -> list | Exception:
        """
        Sets the item status.

        :param value: item value.
        :param status: true/false if active/non active.
        :return: true if the query has been processed successfully.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.value == @value

            UPDATE document WITH {{
                is_active: @status
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'value': value,
                    'status': status
                },
                count = True
            )

            return affected_rows

        except:
            return WhitelistStorageUpdateException()

    def remove(self, value: str, account_id: str) -> Cursor | Exception:
        """
        Removes a whitelist item created by an account.

        :param value: item value.
        :param account_id: a valid account identifier.
        :return: true if the query has been processed successfully.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.value == @value AND
                document.metadata.created_by == @account_id

            REMOVE document IN {self.collection_name}

            RETURN OLD
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'value': value,
                    'account_id': account_id
                },
                count = True
            )

            return affected_rows

        except:
            raise WhitelistStorageRemoveException()

class WhitelistStorageCreateException(Exception):

    """
    Cannot create the whitelist item.
    """

    pass

class WhitelistStorageGetException(Exception):

    """
    Cannot get the whitelist item.
    """

    pass

class WhitelistStorageUpdateException(Exception):

    """
    Cannot update the whitelist item.
    """

    pass

class WhitelistStorageRemoveException(Exception):

    """
    Cannot remove the whitelist item.
    """

    pass
