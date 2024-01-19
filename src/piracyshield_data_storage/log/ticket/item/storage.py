from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class LogTicketItemStorage(DatabaseArangodbDocument):

    collection_name = 'log_ticket_blocking_items'

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
            raise LogTicketItemStorageCreateException()

    def get_all(self, ticket_item_id: str) -> Cursor | Exception:
        """
        Gets all records for a specific ticket item identifier.

        :param ticket_item_id: a valid ticket item identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_item_id == @ticket_item_id

            RETURN {{
                'ticket_item_id': document.ticket_item_id,
                'message': document.message,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_item_id': ticket_item_id
            })

        except:
            raise LogTicketItemStorageGetException()

    def remove_all(self, ticket_item_id: str) -> Cursor | Exception:
        """
        Removes all the records related to a ticket item identifier.

        :param ticket_item_id: a valid ticket item identifier.
        :return: number of affected records.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_item_id == @ticket_item_id

            REMOVE document IN {self.collection_name}

            RETURN OLD
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_item_id': ticket_item_id
                },
                count = True
            )

            return affected_rows

        except:
            raise LogTicketItemStorageRemoveException()

class LogTicketItemStorageCreateException(Exception):

    """
    Cannot create the log.
    """

    pass

class LogTicketItemStorageGetException(Exception):

    """
    Cannot get the log.
    """

    pass

class LogTicketItemStorageRemoveException(Exception):

    """
    Cannot remove the log.
    """

    pass
