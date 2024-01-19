from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class TicketItemStorage(DatabaseArangodbDocument):

    ticket_collection_name = 'ticket_blockings'

    collection_name = 'ticket_blocking_items'

    collection_instance = None

    def __init__(self):
        super().__init__()

        self.collection_instance = self.collection(self.collection_name)

    def insert(self, document: dict) -> dict | Exception:
        """
        Adds a new ticket item.

        :param document: dictionary ticket item data model structure.
        :return: cursor with the inserted data.
        """

        try:
            return self.collection_instance.insert(document)

        except:
            raise TicketItemStorageCreateException()

    def get_all_items_with_genre(self, genre: str) -> Cursor:
        """
        Gets all ticket items. Values only.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.genre == @genre AND
                document.is_active == true

            RETURN DISTINCT document.value
        """

        try:
            return self.query(aql, bind_vars = {
                'genre': genre
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_items_with_genre_by_provider(self, genre: str, provider_id: str) -> Cursor | Exception:
        """
        Gets all ticket items assigned to this provider.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.genre == @genre AND
                document.is_active == true AND
                document.is_duplicate == false AND
                document.is_whitelisted == false AND
                document.is_error == false AND
                document.provider_id == @provider_id

            // ensure only available tickets are considered
            FOR parent_ticket IN {self.ticket_collection_name}
                FILTER
                    parent_ticket.ticket_id == document.ticket_id AND
                    parent_ticket.status.ticket != 'created'

            RETURN DISTINCT document.value
        """

        try:
            return self.query(aql, bind_vars = {
                'genre': genre,
                'provider_id': provider_id
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_items_with_genre_by_ticket(self, ticket_id: str, genre: str) -> Cursor | Exception:
        """
        Gets all ticket items. Values only, by ticket_id.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.genre == @genre AND
                document.is_active == true

            RETURN DISTINCT document.value
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'genre': genre
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_items_with_genre_by_ticket_for_reporter(self, ticket_id: str, genre: str, reporter_id: str) -> Cursor | Exception:
        """
        Gets items of a ticket created by a reporter account.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.genre == @genre

            FOR ticket IN {self.ticket_collection_name}

            FILTER
                ticket.ticket_id == @ticket_id AND
                ticket.metadata.created_by == @reporter_id

            RETURN DISTINCT {{
                'value': document.value,
                'genre': document.genre,
                'is_duplicate': document.is_duplicate,
                'is_whitelisted': document.is_whitelisted,
                'is_error': document.is_error
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'genre': genre,
                'reporter_id': reporter_id
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_items_with_genre_by_ticket_for_provider(self, ticket_id: str, genre: str, provider_id: str) -> Cursor | Exception:
        """
        Gets all ticket items. Values only, by ticket_id.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.genre == @genre AND
                document.is_active == true AND
                document.is_duplicate == false AND
                document.is_whitelisted == false AND
                document.is_error == false AND
                document.provider_id == @provider_id

            // ensure only available tickets are considered
            FOR parent_ticket IN {self.ticket_collection_name}
                FILTER
                    parent_ticket.ticket_id == document.ticket_id AND
                    parent_ticket.status.ticket != 'created'

            RETURN DISTINCT document.value
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'genre': genre,
                'provider_id': provider_id
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_items_available_by_ticket(self, ticket_id: str, account_id: str) -> Cursor | Exception:
        """
        Gets all the ticket items that aren't a duplicate, whitelisted or errors.

        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.is_active == true AND
                document.is_duplicate == false AND
                document.is_whitelisted == false AND
                document.is_error == false

            FOR ticket IN {self.ticket_collection_name}

            FILTER
                ticket.ticket_id == @ticket_id AND
                ticket.metadata.created_by == @account_id

            RETURN DISTINCT document.value
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'account_id': account_id
            })

        except:
            raise TicketItemStorageGetException()

    def get(self, ticket_id: str, value: str) -> Cursor | Exception:
        """
        Gets a single item for a ticket.

        :param ticket_id: ticket identifier.
        :param value: item value.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.value == @value

            RETURN {{
                'ticket_id': document.ticket_id,
                'value': document.value,
                'genre': document.genre,
                'status': document.status,
                'reason': document.reason,
                'provider_id': document.provider_id,
                'created_at': document.created_at,
                'updated_at': document.updated_at
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'value': value
            })

        except:
            raise TicketItemStorageGetException()

    def get_by_value(self, provider_id: str, value: str) -> Cursor | Exception:
        """
        Gets a single item by its value.

        :param ticket_id: ticket identifier.
        :param value: item value.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.provider_id == @provider_id AND
                document.value == @value AND
                document.is_active == true AND
                document.is_duplicate == false AND
                document.is_whitelisted == false AND
                document.is_error == false

            RETURN {{
                'value': document.value,
                'genre': document.genre,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }},
                'settings': document.settings
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'provider_id': provider_id,
                'value': value
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_ticket_items_by(self, ticket_id: str, account_id: str, genre: str, status: str) -> Cursor | Exception:
        """
        Gets all the items filtered by genre and associated account identifier.

        :param ticket_id: ticket identifier.
        :param account_id: provider identifier.
        :param genre: genre of the ticket item.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                account_id == @account_id AND
                genre == @genre AND
                status == @status

            RETURN {{
                'ticket_id': document.ticket_id,
                'value': document.value,
                'genre': document.genre,
                'status': document.status,
                'reason': document.reason,
                'provider_id': document.provider_id,
                'metadata': {{
                    'created_at': document.metadata.created_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'account_id': account_id,
                'genre': genre,
                'status': status
            })

        except:
            raise TicketItemStorageGetException()

    def get_all(self, ticket_id: str) -> Cursor | Exception:
        """
        Gets all the items for a ticket.

        :param ticket_id: ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            RETURN {{
                'ticket_id': document.ticket_id,
                'ticket_item_id': document.ticket_item_id,
                'value': document.value,
                'genre': document.genre,
                'status': document.status,
                'reason': document.reason,
                'provider_id': document.provider_id,
                'metadata': {{
                    'created_at': document.metadata.created_at,
                    'updated_at': document.metadata.updated_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_by_provider(self, provider_id: str, ticket_id: str) -> Cursor | Exception:
        """
        Gets all the items for a ticket by a specific provider_id.

        :param provider_id: the provider_id.
        :param ticket_id: ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.provider_id == @provider_id AND
                document.is_active == true AND
                document.is_duplicate == false AND
                document.is_whitelisted == false AND
                document.is_error == false

            RETURN {{
                'value': document.value,
                'status': document.status,
                'timestamp': document.timestamp,
                'note': document.note,
                'reason': document.reason
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'provider_id': provider_id,
                'ticket_id': ticket_id
            })

        except:
            raise TicketItemStorageGetException()

    def get_details(self, ticket_id: str, ticket_item_id: str) -> Cursor | Exception:
        """
        Gets all the items for a ticket.

        :param ticket_id: ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.ticket_item_id == @ticket_item_id

            LET provider_details = (
                FOR a IN accounts_view

                FILTER a.account_id == document.provider_id

                RETURN {{
                    'account_id': a.account_id,
                    'name': a.name
                }}
            )[0]

            RETURN {{
                'ticket_id': document.ticket_id,
                'ticket_item_id': document.ticket_item_id,
                'value': document.value,
                'genre': document.genre,
                'status': document.status,
                'reason': document.reason,
                'is_active': document.is_active,
                'is_duplicate': document.is_duplicate,
                'is_whitelisted': document.is_whitelisted,
                'is_error': document.is_error,
                'provider': {{
                    'account_id': provider_details.account_id,
                    'name': provider_details.name
                }},
                'metadata': {{
                    'created_at': document.metadata.created_at,
                    'updated_at': document.metadata.updated_at
                }}
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id,
                'ticket_item_id': ticket_item_id
            })

        except:
            raise TicketItemStorageGetException()

    def get_all_status(self, ticket_id: str) -> Cursor | Exception:
        """
        Gets all ticket items statuses.

        :param ticket_id: ticket identifier.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER document.ticket_id == @ticket_id

            RETURN DISTINCT {{
                'ticket_item_id': document.ticket_item_id,
                'value': document.value,
                'genre': document.genre,
                'is_active': document.is_active,
                'is_whitelisted': document.is_whitelisted,
                'is_error': document.is_error
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'ticket_id': ticket_id
            })

        except:
            raise TicketItemStorageGetException()

    def exists_by_value(self, genre: str, value: str) -> Cursor | Exception:
        """
        Searches for a duplicate.

        :param genre: the genre of the ticket item.
        :param genre: the value of the ticket item.
        :return: cursor with the requested data.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.genre == @genre AND
                document.value == @value AND
                document.is_active == true AND
                document.is_duplicate == false AND
                document.is_whitelisted == false

            RETURN DISTINCT {{
                'ticket_id': document.ticket_id,
                'genre': document.genre,
                'value': document.value
            }}
        """

        try:
            return self.query(aql, bind_vars = {
                'genre': genre,
                'value': value
            })

        except:
            raise TicketItemStorageGetException()

    def update_status_by_value(self,
        provider_id: str,
        value: str,
        status: str,
        updated_at: str,
        status_reason: str = None,
        timestamp: str = None,
        note: str = None
    ) -> list | Exception:
        """
        Sets the item status by its value.

        :param provider_id: the id of the provider account.
        :param value: item value.
        :param status: item status value.
        :param updated_at: a timestamp of the update date.
        :param status_reason: a valid predefined reason for unprocessed items.
        :param timestamp: a timestamp of the update date set by the provider account.
        :param note: a generic text set by the provider account.
        :return: true if the query has been processed successfully.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.value == @value AND
                document.provider_id == @provider_id AND
                document.is_active == true AND
                document.is_duplicate == false AND
                document.is_whitelisted == false AND
                document.is_error == false

            // prevent editing a ticket item with a non workable ticket
            FOR parent_ticket IN {self.ticket_collection_name}
                FILTER
                    parent_ticket.ticket_id == document.ticket_id AND
                    parent_ticket.status.ticket != 'created'

            UPDATE document WITH {{
                status: @status,
                reason: @status_reason,
                timestamp: @timestamp,
                note: @note,
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
                    'provider_id': provider_id,
                    'value': value,
                    'status': status,
                    'updated_at': updated_at,
                    'status_reason': status_reason,
                    'timestamp': timestamp,
                    'note': note
                },
                count = True
            )

            return affected_rows

        except:
            raise TicketItemStorageUpdateException()

    def set_flag_active(self, value: str, status: str) -> list | Exception:
        """
        Sets the item activity status.

        :param value: item value.
        :param status: item status value.
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
            raise TicketItemStorageUpdateException()

    def set_flag_error(self, ticket_id: str, value: str, status: bool) -> list | Exception:
        """
        Sets the error flag.

        :param ticket_id: a valid ticket identifier.
        :param value: item value.
        :param status: true or false if status is error or not.
        :return: a list of affected rows.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.value == @value

            UPDATE document WITH {{
                is_error: @status
            }} IN {self.collection_name}

            RETURN NEW
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_id': ticket_id,
                    'value': value,
                    'status': status
                },
                count = True
            )

            return affected_rows

        except:
            raise TicketItemStorageUpdateException()

    def remove(self, ticket_id: str, value: str) -> list | Exception:
        """
        Removes a ticket item.

        :param ticket_id: ticket identifier.
        :param value: item to remove.
        :return: true if the query has been processed successfully.
        """

        aql = f"""
            FOR document IN {self.collection_name}

            FILTER
                document.ticket_id == @ticket_id AND
                document.value == @value

            REMOVE document IN {self.collection_name}

            RETURN OLD
        """

        try:
            affected_rows = self.query(
                aql,
                bind_vars = {
                    'ticket_id': ticket_id,
                    'value': value
                },
                count = True
            )

            return affected_rows

        except:
            raise TicketItemStorageRemoveException()

    def remove_all(self, ticket_id: str) -> list | Exception:
        """
        Removes all the ticket items.

        :param ticket_id: ticket identifier.
        :param value: item to remove.
        :return: true if the query has been processed successfully.
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
            raise TicketItemStorageRemoveException()

class TicketItemStorageCreateException(Exception):

    """
    Cannot create the ticket item.
    """

    pass

class TicketItemStorageGetException(Exception):

    """
    Cannot get the ticket item.
    """

    pass

class TicketItemStorageUpdateException(Exception):

    """
    Cannot update the ticket item.
    """

    pass

class TicketItemStorageRemoveException(Exception):

    """
    Cannot remove the ticket item.
    """

    pass
