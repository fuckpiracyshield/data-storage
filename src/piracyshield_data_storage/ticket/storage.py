from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class TicketStorage(DatabaseArangodbDocument):

    ticket_item_collection_name = 'ticket_blocking_items'

    collection_name = 'ticket_blockings'

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
            raise TicketStorageCreateException()

    def get(self, ticket_id: str) -> Cursor | Exception:
        """
        Gets a single ticket document.

        :param ticket_id: ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            LET created_by_name = (
                FOR a IN accounts_view
                FILTER a.account_id == document.metadata.created_by
                RETURN a.name
            )[0]

            LET assigned_to_details = (
                FOR i IN document.assigned_to

                    FOR a IN accounts_view

                    FILTER a.account_id == i

                    RETURN {{
                        'account_id': a.account_id,
                        'name': a.name
                    }}
            )

            RETURN {{
                'ticket_id': document.ticket_id,
                'dda_id': document.dda_id,
                'description': document.description,
                'fqdn': document.fqdn,
                'ipv4': document.ipv4,
                'ipv6': document.ipv6,
                'assigned_to': assigned_to_details,
                'status': document.status,
                'metadata': {{
                    'created_at': document.metadata.created_at,
                    'updated_at': document.metadata.updated_at,
                    'created_by': document.metadata.created_by,
                    'created_by_name': created_by_name
                }},
                'settings': document.settings,
                'tasks': document.tasks
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id
            })

        except:
            raise TicketStorageGetException()

    def has_dda_id(self, dda_id: str) -> Cursor | Exception:
        """
        Finds a ticket with the specified DDA identifier.

        :param dda_id: a valid DDA identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.dda_id == @dda_id

            LIMIT 1

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'dda_id': dda_id
            })

        except:
            raise TicketStorageGetException()

    def get_reporter(self, ticket_id: str, account_id: str) -> Cursor | Exception:
        """
        Gets a single ticket document with limitations.

        :param ticket_id: ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.metadata.created_by == @account_id

            RETURN {{
                'ticket_id': document.ticket_id,
                'dda_id': document.dda_id,
                'description': document.description,
                'fqdn': document.fqdn,
                'ipv4': document.ipv4,
                'ipv6': document.ipv6,
                'status': document.status,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }},
                'settings': {{
                    'revoke_time': document.settings.revoke_time,
                    'autoclose_time': document.settings.autoclose_time,
                    'report_error_time': document.settings.report_error_time
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'account_id': account_id
            })

        except:
            raise TicketStorageGetException()

    def get_provider(self, ticket_id: str, account_id: str) -> Cursor | Exception:
        """
        Gets a single ticket document with limitations.

        :param ticket_id: ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                (document.status == 'open' OR document.status == 'closed') AND
                POSITION(document.assigned_to, @account_id) == true

            LET ticket_items = (
                FOR ticket_item in {self.ticket_item_collection_name}

                FILTER
                    ticket_item.ticket_id == document.ticket_id AND
                    ticket_item.is_active == true AND
                    ticket_item.is_duplicate == false AND
                    ticket_item.is_whitelisted == false AND
                    ticket_item.is_error == false

                COLLECT genre = ticket_item.genre INTO groupedItems
                FILTER genre IN ["fqdn", "ipv4", "ipv6"]
                RETURN {{
                    "genre": genre,
                    "items": UNIQUE(groupedItems[*].ticket_item.value)
                }}
            )

            FILTER LENGTH(ticket_items) != 0

            LET fqdn_items = (FOR item IN ticket_items FILTER item.genre == 'fqdn' RETURN item.items)
            LET ipv4_items = (FOR item IN ticket_items FILTER item.genre == 'ipv4' RETURN item.items)
            LET ipv6_items = (FOR item IN ticket_items FILTER item.genre == 'ipv6' RETURN item.items)

            RETURN {{
                'ticket_id': document.ticket_id,
                'fqdn': fqdn_items[0] or [],
                'ipv4': ipv4_items[0] or [],
                'ipv6': ipv6_items[0] or [],
                'status': document.status,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'account_id': account_id
            })

        except:
            raise TicketStorageGetException()

    def get_all(self) -> Cursor | Exception:
        """
        Fetches all the tickets.

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

            SORT document.metadata.created_at DESC

            RETURN {{
                'ticket_id': document.ticket_id,
                'description': document.description,
                'fqdn': document.fqdn,
                'ipv4': document.ipv4,
                'ipv6': document.ipv6,
                'assigned_to': document.assigned_to,
                'status': document.status,
                'metadata': {{
                    'created_at': document.metadata.created_at,
                    'updated_at': document.metadata.updated_at,
                    'created_by': document.metadata.created_by,
                    'created_by_name': created_by_name
                }},
                'settings': document.settings,
                'tasks': document.tasks
            }}
        """

        try:
            return self.query(aql)

        except:
            raise TicketStorageGetException()

    def get_all_reporter(self, account_id: str) -> Cursor | Exception:
        """
        Fetches all the tickets for a single account.

        :param account_id: the identifier of the ticket's creator.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.metadata.created_by == @account_id

            SORT document.metadata.created_at DESC

            RETURN {{
                'ticket_id': document.ticket_id,
                'description': document.description,
                'fqdn': document.fqdn,
                'ipv4': document.ipv4,
                'ipv6': document.ipv6,
                'status': document.status,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }},
                'settings': {{
                    'revoke_time': document.settings.revoke_time
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'account_id': account_id
            })

        except:
            raise TicketStorageGetException()

    def get_all_provider(self, account_id) -> Cursor | Exception:
        """
        Fetches all the tickets with limited parameters.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                (document.status == 'open' OR document.status == 'closed') AND
                POSITION(document.assigned_to, @account_id) == true

            LET ticket_items = (
                FOR ticket_item in {self.ticket_item_collection_name}

                FILTER
                    ticket_item.ticket_id == document.ticket_id AND
                    ticket_item.is_active == true AND
                    ticket_item.is_duplicate == false AND
                    ticket_item.is_whitelisted == false AND
                    ticket_item.is_error == false

                COLLECT genre = ticket_item.genre INTO groupedItems
                FILTER genre IN ["fqdn", "ipv4", "ipv6"]
                RETURN {{
                    "genre": genre,
                    "items": UNIQUE(groupedItems[*].ticket_item.value)
                }}
            )

            FILTER LENGTH(ticket_items) != 0

            LET fqdn_items = (FOR item IN ticket_items FILTER item.genre == 'fqdn' RETURN item.items)
            LET ipv4_items = (FOR item IN ticket_items FILTER item.genre == 'ipv4' RETURN item.items)
            LET ipv6_items = (FOR item IN ticket_items FILTER item.genre == 'ipv6' RETURN item.items)

            SORT document.metadata.created_at DESC

            RETURN {{
                'ticket_id': document.ticket_id,
                'fqdn': fqdn_items[0] or [],
                'ipv4': ipv4_items[0] or [],
                'ipv6': ipv6_items[0] or [],
                'status': document.status,
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
            raise TicketStorageGetException()

    def get_total(self) -> int | Exception:
        """
        Total documents in the tickets collection.

        :return: total number of documents.
        """

        try:
            return self.collection_instance.count()

        except:
            raise TicketStorageGetException()

    def exists_by_identifier(self, ticket_id: str) -> Cursor | Exception:
        """
        Checks if a ticket with this identifier is in the collection.

        :param value: a valid ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            LIMIT 1

            RETURN document
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id
            })

        except:
            raise TicketStorageGetException()

    def update_task_list(self, ticket_id: str, task_ids: list, updated_at: str) -> Cursor | Exception:
        """
        Appends a new task.

        :param ticket_id: ticket identifier.
        :param task_ids: a list of tasks identifiers.
        :return: list of updated rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}
            FILTER document.ticket_id == @ticket_id

            UPDATE document WITH {{
                "tasks": APPEND(document.tasks, @task_ids, true),
                "metadata": {{
                    "updated_at": @updated_at
                }}
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_id': ticket_id,
                    'task_ids': task_ids,
                    'updated_at': updated_at
                },
                count = True
            )

            return affected_rows

        except:
            raise TicketStorageUpdateException()

    def update_status(self, ticket_id: str, ticket_status: str) -> Cursor | Exception:
        """
        Sets the ticket status.
        Used by tasks to update from `CREATED` to `OPEN` and from `OPEN` to `CLOSED` once each time expires.

        :param ticket_id: ticket identifier.
        :param ticket_status: ticket status value.
        :return: list of updated rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            UPDATE document WITH {{
                'status': @ticket_status
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_id': ticket_id,
                    'ticket_status': ticket_status
                },
                count = True
            )

            return affected_rows

        except:
            raise TicketStorageUpdateException()

    def remove(self, ticket_id: str) -> Cursor | Exception:
        """
        Removes a ticket.

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

class TicketStorageCreateException(Exception):

    """
    Cannot create the ticket.
    """

    pass

class TicketStorageGetException(Exception):

    """
    Cannot get the ticket.
    """

    pass

class TicketStorageUpdateException(Exception):

    """
    Cannot update the ticket.
    """

    pass

class TicketStorageRemoveException(Exception):

    """
    Cannot remove the ticket.
    """

    pass
