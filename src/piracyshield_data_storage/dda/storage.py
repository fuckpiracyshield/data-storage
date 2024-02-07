from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class DDAStorage(DatabaseArangodbDocument):

    collection_name = 'dda_instances'

    collection_instance = None

    def __init__(self):
        super().__init__()

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict | Exception:
        """
        Adds a new DDA identifier.

        :param document: dictionary with the expected data model values.
        :return: cursor with the inserted data.
        """

        try:
            return self.collection_instance.insert(document)

        except:
            raise DDAStorageCreateException()

    def get_by_identifier(self, dda_id: str) -> Cursor | Exception:
        """
        Fetches a single DDA instance by its identifier.

        :param dda_id: a valid DDA identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.dda_id == @dda_id

            RETURN {{
                'dda_id': document.dda_id,
                'description': document.description,
                'instance': document.instance,
                'is_active': document.is_active,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'dda_id': dda_id
            })

        except:
            raise DDAStorageGetException()

    def get_by_identifier_for_reporter(self, dda_id: str, reporter_id: str) -> Cursor | Exception:
        """
        Fetches a single DDA instance by its identifier for reporter.

        :param dda_id: a valid DDA identifier.
        :param account_id: a valid reporter identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.dda_id == @dda_id AND
                document.account_id == @reporter_id

            RETURN {{
                'dda_id': document.dda_id,
                'description': document.description,
                'instance': document.instance,
                'is_active': document.is_active,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'dda_id': dda_id,
                'reporter_id': reporter_id
            })

        except:
            raise DDAStorageGetException()

    def get_global(self) -> Cursor | Exception:
        """
        Fetches all the DDA instances.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            // resolve account identifier
            LET account_name = (
                FOR a IN accounts_view
                FILTER a.account_id == document.account_id
                RETURN a['name']
            )[0]

            SORT document.instance ASC

            RETURN {{
                'dda_id': document.dda_id,
                'description': document.description,
                'instance': document.instance,
                'account_id': document.account_id,
                'account_name': account_name,
                'is_active': document.is_active,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql)

        except:
            raise DDAStorageGetException()

    def get_all_by_account(self, account_id: str) -> Cursor | Exception:
        """
        Fetches all the DDA instances assigned to an account.

        :param account_id: a valid account identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.account_id == @account_id

            SORT document.instance ASC

            RETURN {{
                'dda_id': document.dda_id,
                'description': document.description,
                'instance': document.instance,
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
            raise DDAStorageGetException()

    def exists_by_instance(self, instance: str) -> Cursor | Exception:
        """
        Searches for an instance.

        :param instance: a valid DDA instance.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.instance == @instance

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'instance': instance
            })

        except:
            raise DDAStorageGetException()

    def is_assigned_to_account(self, dda_id: str, account_id: str) -> Cursor | Exception:
        """
        Searches for a DDA identifier assigned to a specified account identifier.

        :param dda_id: a valid DDA identifier.
        :param account_id: a valid account identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.dda_id == @dda_id AND
                document.account_id == @account_id AND
                document.is_active == True

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'dda_id': dda_id,
                'account_id': account_id
            })

        except:
            raise DDAStorageGetException()

    def update_status(self, dda_id: str, status: bool) -> list | Exception:
        """
        Sets the DDA identifier status.

        :param dda_id: a valid DDA identifier.
        :param status: true/false if active/non active.
        :return: true if the query has been processed successfully.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.dda_id == @dda_id

            UPDATE document WITH {{
                is_active: @status
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'dda_id': dda_id,
                    'status': status
                },
                count = True
            )

            return affected_rows

        except:
            return DDAStorageUpdateException()

    def remove(self, dda_id: str) -> Cursor | Exception:
        """
        Removes a DDA identifier.

        :param dda_id: DDA identifier.
        :return: true if the query has been processed successfully.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.dda_id == @dda_id

            REMOVE document IN {self.collection_name}

            RETURN OLD
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'dda_id': dda_id
                },
                count = True
            )

            return affected_rows

        except:
            raise DDAStorageRemoveException()

class DDAStorageCreateException(Exception):

    """
    Cannot create the DDA item.
    """

    pass

class DDAStorageGetException(Exception):

    """
    Cannot get the DDA item.
    """

    pass

class DDAStorageUpdateException(Exception):

    """
    Cannot update the DDA item.
    """

    pass

class DDAStorageRemoveException(Exception):

    """
    Cannot remove the DDA item.
    """

    pass
