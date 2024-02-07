from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class ForensicStorage(DatabaseArangodbDocument):

    collection_name = 'forensics'

    collection_instance = None

    def __init__(self):
        super().__init__()

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict | Exception:
        """
        Adds a new ticket.

        :param document: dictionary with the expected ticket data model values.
        :return: cursor with the inserted data.
        """

        try:
            return self.collection_instance.insert(document)

        except:
            raise ForensicStorageCreateException()

    def exists_ticket_id(self, ticket_id: str) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            COLLECT WITH COUNT INTO length

            RETURN length
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id
            })

        except:
            raise ForensicStorageGetException()

    def exists_hash_string(self, hash_string: str) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.hash_string == @hash_string

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'hash_string': hash_string
            })

        except:
            raise ForensicStorageGetException()

    def get_by_ticket(self, ticket_id: str) -> Cursor | Exception:
        """
        Returns forensic document by ticket identifier.

        :param ticket_id: a ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id
            })

        except:
            raise ForensicStorageGetException()

    def get_by_ticket_for_reporter(self, ticket_id: str, reporter_id: str) -> Cursor | Exception:
        """
        Returns forensic document by ticket identifier.

        :param ticket_id: a ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.metadata.created_by == @reporter_id

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'reporter_id': reporter_id
            })

        except:
            raise ForensicStorageGetException()

    def update_archive_name(self, ticket_id: str, archive_name: str, status: str, updated_at: str) -> Cursor | Exception:
        """
        Insert the forensic evidence archive name.

        :param ticket_id: ticket identifier.
        :param archive_name: name of the file archive.
        :param status: status of the analysis.
        :param updated_at: date of this update.
        :return: list of updated rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            UPDATE document WITH {{
                archive_name: @archive_name,
                status: @status,
                metadata: {{
                    updated_at: @updated_at
                }}
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_id': ticket_id,
                    'archive_name': archive_name,
                    'status': status,
                    'updated_at': updated_at
                },
                count = True
            )

            return affected_rows

        except:
            raise ForensicStorageUpdateException()

    def update_archive_status(self, ticket_id: str, status: str, updated_at: str, reason: str = None) -> Cursor | Exception:
        """
        Updates the status of a previously created record.

        :param ticket_id: ticket identifier.
        :param status: status of the analysis.
        :param reason: additional string message.
        :param updated_at: date of this update.
        :return: list of updated rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            UPDATE document WITH {{
                status: @status,
                reason: @reason,
                metadata: {{
                    updated_at: @updated_at
                }}
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_id': ticket_id,
                    'status': status,
                    'reason': reason,
                    'updated_at': updated_at
                },
                count = True
            )

            return affected_rows

        except:
            raise ForensicStorageUpdateException()

    def remove_by_ticket(self, ticket_id: str) -> Cursor | Exception:
        """
        Removes a forensic archive.

        :param ticket_id: ticket identifier.
        :return: list of removed rows.
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
            raise TicketStorageRemoveException()

class ForensicStorageCreateException(Exception):

    """
    Cannot create the forensic archive.
    """

    pass

class ForensicStorageGetException(Exception):

    """
    Cannot get the forensic archive.
    """

    pass

class ForensicStorageUpdateException(Exception):

    """
    Cannot update the forensic archive.
    """

    pass

class ForensicStorageRemoveException(Exception):

    """
    Cannot remove the forensic archive.
    """

    pass
