from piracyshield_data_storage.database.arangodb.document import DatabaseArangodbDocument

from arango.cursor import Cursor

class GeneralAccountStorage(DatabaseArangodbDocument):

    view_name = 'accounts_view'

    def __init__(self):
        super().__init__()

    def get(self, account_id: str) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.view_name}

            FILTER document.account_id == @account_id

            // resolve account identifier
            LET created_by_name = (
                FOR a IN accounts_view
                FILTER a.account_id == document.metadata.created_by
                RETURN a['name']
            )[0]

            RETURN {{
                'account_id': document.account_id,
                'name': document.name,
                'email': document.email,
                'role': document.role,
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
            return self.query(aql, bind_vars = {
                'account_id': account_id
            })

        except:
            raise GeneralAccountStorageGetException()

    def get_all(self) -> Cursor | Exception:
        aql = f"""
            FOR document IN {self.view_name}

            // resolve account identifier
            LET created_by_name = (
                FOR a IN accounts_view
                FILTER a.account_id == document.metadata.created_by
                RETURN a['name']
            )[0]

            RETURN {{
                'account_id': document.account_id,
                'name': document.name,
                'email': document.email,
                'role': document.role,
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
            raise GeneralAccountStorageGetException()

class GeneralAccountStorageGetException(Exception):

    """
    Cannot get the account.
    """

    pass
