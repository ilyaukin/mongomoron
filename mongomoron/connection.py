import logging
import threading
from typing import Union, Any, Iterable, Optional, Callable

import pymongo
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import InsertOneResult, InsertManyResult

from mongomoron.expression import Collection, QueryBuilder, InsertBuilder, \
    UpdateBuilder, DeleteBuilder, AggregationPipelineBuilder, IndexBuilder

logger = logging.getLogger(__name__)


class Operation:
    """
    Operations that can be hooked
    """
    INSERT = 'insert'
    UPDATE = 'update'
    DELETE = 'delete'


class DatabaseConnection(object):
    """
    A wrapper for mongo Database, to do actual
    operations with the database
    """

    def __init__(self, mongo_client: pymongo.MongoClient = None, db: Database = None):
        self._mongo_client = mongo_client
        self._db = db
        self._hook_map = {}
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
            for hook in self._hook_map.get(
                    (Operation.INSERT, builder.collection._name), []):
                hook(builder.documents)
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
            for hook in self._hook_map.get(
                    (Operation.UPDATE, builder.collection._name), []):
                hook(builder.filter_expression, builder.update_operators)
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
            for hook in self._hook_map.get(
                    (Operation.DELETE, builder.collection._name), []):
                hook(builder.filter_expression)
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

    def add_hook(self, operation: str, collection_list: Iterable[Collection],
                 hook: Union[Callable, None] = None) -> Union[Callable[[Callable], None], None]:
        """
        Add a hook on some operation and some collection(s).
        Hook is executed before each corresponding operation.
        Example use case for that is to automatically add datetime_created
        and datetime_updated to each record.

        :param operation Any of `Operation.INSERT`, `Operation.UPDATE`,
        `Operation.DELETE`
        :param collection_list List of collections to apply the hook to
        :param hook a hook function. For INSERT, receives a list of documents.
        For UPDATE, receives filter in mongodb syntax, and update operator in
        mongodb syntax. For DELETE, receives filter in mongodb syntax.
        If not passed, this method returns a function to set a hook that
        can be used as decorator
        """
        if not hook:
            return lambda hook: self.add_hook(operation, collection_list, hook)

        for collection in collection_list:
            self._hook_map.setdefault((operation, collection._name), [])
            self._hook_map[operation, collection._name].append(hook)

    def _collection_exists(self, collection_name: str) -> bool:
        return collection_name in self.db().list_collection_names()
