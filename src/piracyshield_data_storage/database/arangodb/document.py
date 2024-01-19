from piracyshield_data_storage.database.arangodb.connection import DatabaseArangodbConnection

from arango.exceptions import AQLQueryExecuteError

class DatabaseArangodbDocument(DatabaseArangodbConnection):

    def collection(self, collection):
        try:
            return self.instance.collection(collection)

        except:
            raise DatabaseArangodbCollectionNotFoundException()

    def query(self, aql, **kwargs):
        try:
            return self.instance.aql.execute(aql, **kwargs)

        except AQLQueryExecuteError:
            raise DatabaseArangodbQueryException()

class DatabaseArangodbCollectionNotFoundException(Exception):

    pass

class DatabaseArangodbQueryException(Exception):

    pass
