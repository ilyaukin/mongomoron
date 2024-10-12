import logging
import threading
from typing import Union, Any

import pymongo
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import InsertOneResult, InsertManyResult

from mongomoron.expression import Collection, QueryBuilder, InsertBuilder, \
    UpdateBuilder, DeleteBuilder, AggregationPipelineBuilder, IndexBuilder

logger = logging.getLogger(__name__)


class DatabaseConnection(object):
    """
    A wrapper for mongo Database, to do actual
    operations with the database
    """

    def __init__(self, mongo_client: pymongo.MongoClient, db: Database):
        self._mongo_client = mongo_client
        self._db = db
        self.threadlocal = threading.local()
        self.threadlocal.session = None

    def mongo_client(self):
        return self._mongo_client

    def db(self) -> Database:
        """
        Using db() instead of _db among the code
        to be able override this class in cases when database
        provided by other ways rather then in constructor
        """
        return self._db

    def session(self):
        return getattr(self.threadlocal, 'session', None)

    def create_collection(self, collection: Union['Collection', str],
                          override: bool = True) -> 'Collection':
        collection_name = collection._name if isinstance(collection,
                                                         Collection) else \
            collection
        if override and self._collection_exists(collection_name):
            self.db()[collection_name].drop(session=self.session())
        self.db().create_collection(collection_name, session=self.session())
        return Collection(collection_name)

    def create_index(self, builder: IndexBuilder):
        self.db()[builder.collection._name].create_index(builder.keys,
                                                         unique=builder.is_unique,
                                                         session=self.session())

    def drop_collection(self, collection: Union['Collection', str]):
        collection_name = collection._name if isinstance(collection,
                                                         Collection) else \
            collection
        self.db()[collection_name].drop(session=self.session())

    def execute(self, builder: 'Executable') -> Union[
        Cursor, dict, InsertOneResult, InsertManyResult, Any]:
        if isinstance(builder, QueryBuilder):
            if builder.one:
                logger.debug('db.%s.find_one(%s)', builder.collection._name,
                             builder.query_filer_document)
                return self.db()[builder.collection._name].find_one(
                    builder.query_filer_document)
            else:
                logger.debug('db.%s.find(%s)', builder.collection._name,
                             builder.query_filer_document)
                cursor = self.db()[builder.collection._name].find(
                    builder.query_filer_document)
                if builder.sort_list:
                    cursor.sort(builder.sort_list)
                return cursor
        elif isinstance(builder, InsertBuilder):
            if builder.one:
                logger.debug('db.%s.insert_one(%s)', builder.collection._name,
                             builder.documents[0])
                return self.db()[builder.collection._name].insert_one(
                    builder.documents[0], session=self.session())
            else:
                logger.debug('db.%s.insert_many(%s)', builder.collection._name,
                             [builder.documents[0], '...'] if len(
                                 builder.documents) > 1 else builder.documents)
                return self.db()[builder.collection._name].insert_many(
                    builder.documents, session=self.session())
        elif isinstance(builder, UpdateBuilder):
            if builder.one:
                logger.debug('db.%s.update_one(%s, %s)',
                             builder.collection._name,
                             builder.filter_expression,
                             builder.update_operators)
                return self.db()[builder.collection._name].update_one(
                    builder.filter_expression, builder.update_operators,
                    upsert=builder.upsert,
                    session=self.session())
            else:
                logger.debug('db.%s.update(%s, %s)', builder.collection._name,
                             builder.filter_expression,
                             builder.update_operators)
                return self.db()[builder.collection._name].update_many(
                    builder.filter_expression, builder.update_operators,
                    upsert=builder.upsert, session=self.session())
        elif isinstance(builder, DeleteBuilder):
            logger.debug('db.%s.delete_many(%s)', builder.collection._name,
                         builder.filter_expression)
            return self.db()[builder.collection._name].delete_many(
                builder.filter_expression, session=self.session())
        elif isinstance(builder, AggregationPipelineBuilder):
            pipeline = builder.get_pipeline()
            logger.debug('db.%s.aggregate(%s)', builder.collection._name,
                         pipeline)
            return self.db()[builder.collection._name].aggregate(pipeline,
                                                                 session=self.session())
        else:
            raise NotImplementedError(
                'Execution of %s not implemented' % type(builder))

    def transactional(self, foo):
        """
        Decorator to do a method in the transaction.
        Session is stored in thread local
        @param foo: Function to be decorated
        @return: Decorated function
        """

        def foo_in_transaction(*args, **kwargs):
            self.threadlocal.session = self.mongo_client().start_session()
            self.threadlocal.session.start_transaction()
            try:
                result = foo(*args, **kwargs)
                self.threadlocal.session.commit_transaction()
                return result
            except Exception as e:
                self.threadlocal.session.abort_transaction()
                raise e
            finally:
                self.threadlocal.session = None

        foo_in_transaction.__name__ = foo.__name__
        return foo_in_transaction

    def _collection_exists(self, collection_name: str) -> bool:
        return collection_name in self.db().list_collection_names()
