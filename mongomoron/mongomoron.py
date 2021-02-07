import pymongo
from pymongo.database import Database


class DatabaseConnection(object):
    """
    A wrapper for mongo Database, to do actual
    operations with the database
    """

    def __init__(self, db: Database):
        self.db = db

    def create_collection(self, collection_name: str,
                          override: bool = True) -> 'Table':
        if override and self._collection_exists(collection_name):
            self.db[collection_name].drop()
        self.db.create_collection(self, collection_name)
        return Table(collection_name)

    def create_index(self, index: 'IndexBuilder'):
        self.db[index.table.name].create_index(index.keys)

    def _collection_exists(self, collection_name: str) -> bool:
        return collection_name in self.db.list_collection_names()


class Table(object):
    """
    A table object which hosts fields.
    Any attribute of this object is considered as a field
    with the corresponding name.
    """

    def __init__(self, name: str):
        self.name = name

    def __getattr__(self, item) -> 'Field':
        return Field(item)


class IndexBuilder(object):
    """
    Index object builder, to execute create_index
    """

    def __init__(self, table: Table):
        self.table = table
        self.keys = []

    def asc(self, field: str) -> 'IndexBuilder':
        self.keys.append((field, pymongo.ASCENDING))
        return self

    def desc(self, field: str):
        self.keys.append((field, pymongo.DESCENDING))
        return self


def index(table: Table) -> IndexBuilder:
    return IndexBuilder(table)


class Expression(object):
    """
    An expression.

    https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#aggregation-expressions
    """

    def to_obj(self):
        raise NotImplementedError('Must be implemented in subclasses')


class Field(Expression):
    """
    A field path expression
    """

    def __init__(self, name: str):
        self.name = name

    def to_obj(self):
        return '$' + self.name
