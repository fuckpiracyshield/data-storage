from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class TicketErrorStorage(DatabaseArangodbDocument):

    collection_name = 'ticket_errors'

    collection_instance = None

    def __init__(self):
        super().__init__()

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict | Exception:
        """
        Adds a new error ticket.

        :param document: dictionary with the expected ticket data model values.
        :return: cursor with the inserted data.
        """

        try:
            return self.collection_instance.insert(document)

        except:
            raise TicketErrorStorageCreateException()

    def get(self, ticket_error_id: str) -> Cursor | Exception:
        """
        Gets error ticket by its identifier.

        :param ticket_error_id: a valid ticket error identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_error_id == @ticket_error_id

            LET created_by_name = (
                FOR a IN accounts_view
                FILTER a.account_id == document.metadata.created_by
                RETURN a.name
            )[0]

            RETURN {{
                'ticket_error_id': document.ticket_error_id,
                'ticket_id': document.ticket_id,
                'fqdn': document.fqdn,
                'ipv4': document.ipv4,
                'ipv6': document.ipv6,
                'metadata': {{
                    'created_at': document.metadata.created_at,
                    'created_by': document.metadata.created_by,
                    'created_by_name': created_by_name
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_error_id': ticket_error_id
            })

        except:
            raise TicketErrorStorageGetException()

    def get_by_reporter(self, ticket_error_id: str, reporter_id: str) -> Cursor | Exception:
        """
        Gets error ticket by its identifier and creator account.

        :param ticket_error_id: a valid ticket error identifier.
        :param reporter_id: a valid reporter account identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_error_id == @ticket_error_id AND
                document.metadata.created_by == @reporter_id

            RETURN {{
                'ticket_error_id': document.ticket_error_id,
                'ticket_id': document.ticket_id,
                'fqdn': document.fqdn,
                'ipv4': document.ipv4,
                'ipv6': document.ipv6,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_error_id': ticket_error_id,
                'reporter_id': reporter_id
            })

        except:
            raise TicketErrorStorageGetException()

    def get_by_ticket(self, ticket_id: str) -> Cursor | Exception:
        """
        Gets error tickets by ticket identifier.

        :param ticket_id: a valid ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            RETURN {{
                'ticket_error_id': document.ticket_error_id,
                'fqdns': document.fqdn or [],
                'ipv4': document.ipv4 or [],
                'ipv6': document.ipv6 or [],
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
            raise TicketErrorStorageGetException()

class TicketErrorStorageCreateException(Exception):

    """
    Cannot create the error ticket.
    """

    pass

class TicketErrorStorageGetException(Exception):

    """
    Cannot get the error ticket.
    """

    pass
