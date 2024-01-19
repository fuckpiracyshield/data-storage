from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class LogTicketStorage(DatabaseArangodbDocument):

    collection_name = 'log_ticket_blockings'

    collection_instance = None

    def __init__(self):
        super().__init__()

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict | Exception:
        """
        Adds a new log record.

        :param document: dictionary with the values to insert.
        :return: cursor with the inserted data.
        """

        try:
            return self.collection_instance.insert(document)

        except:
            raise LogTicketStorageCreateException()

    def get_all(self, ticket_id: str) -> Cursor | Exception:
        """
        Gets all records for a specific ticket identifier.

        :param ticket_id: a valid ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            RETURN {{
                'ticket_id': document.ticket_id,
                'message': document.message,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id
            })

        except:
            raise LogTicketStorageGetException()

    def remove_all(self, ticket_id: str) -> Cursor | Exception:
        """
        Removes all the records related to a ticket identifier.

        :param ticket_id: a valid ticket identifier.
        :return: number of affected records.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            REMOVE document IN {self.collection_name}

            RETURN OLD
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_id': ticket_id
                },
                count = True
            )

            return affected_rows

        except:
            raise LogTicketStorageRemoveException()

class LogTicketStorageCreateException(Exception):

    """
    Cannot create the log.
    """

    pass

class LogTicketStorageGetException(Exception):

    """
    Cannot get the log.
    """

    pass

class LogTicketStorageRemoveException(Exception):

    """
    Cannot remove the log.
    """

    pass
