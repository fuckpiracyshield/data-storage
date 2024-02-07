from piracyshield_data_storage.database.arangodb.connection import DatabaseArangodbConnection

from arango.exceptions import AQLQueryExecuteError

class DatabaseArangodbDocument(DatabaseArangodbConnection):

    max_batch_size = 50000

    def collection(self, collection):
        try:
            return self.instance.collection(collection)

        except:
            raise DatabaseArangodbCollectionNotFoundException()

    def query(self, aql, **kwargs):
        try:
            return self.instance.aql.execute(aql, batch_size = self.max_batch_size, **kwargs)

        except AQLQueryExecuteError:
            raise DatabaseArangodbQueryException()

class DatabaseArangodbCollectionNotFoundException(Exception):

    pass

class DatabaseArangodbQueryException(Exception):

    pass
