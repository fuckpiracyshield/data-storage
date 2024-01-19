from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class LogStorage(DatabaseArangodbDocument):

    collection_name = 'logs'

    collection_instance = None

    def __init__(self):
        super().__init__()

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict:
        """
        Adds a new log record.

        :param document: dictionary with the values to insert.
        :return: cursor with the inserted data.
        """

        try:
            return self.collection_instance.insert(document)

        except:
            raise LogStorageCreateException()

    def get(self, identifier: str) -> Cursor:
        """
        Gets log records for a specific identifier.

        :param identifier: generic identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.identifier == @identifier

            RETURN {{
                'log_id': document.log_id,
                'time': document.identifier,
                'message': document.message,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'identifier': identifier
            })

        except:
            raise LogStorageGetException()

    def remove_by_ticket_id(self, ticket_id: str) -> dict | bool:
        """
        Removes all the logs related to a ticket.

        :param ticket_id: ticket identifier.
        :return: true if the query has been processed successfully.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.identifier == @ticket_id
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
            raise LogStorageRemoveException()

class LogStorageCreateException(Exception):

    """
    Cannot create the log.
    """

    pass

class LogStorageGetException(Exception):

    """
    Cannot get the log.
    """

    pass

class LogStorageRemoveException(Exception):

    """
    Cannot remove the log.
    """

    pass
