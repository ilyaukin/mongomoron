import logging
import threading
from typing import Iterable, Union, Any, Optional, Callable, Tuple

import pymongo
from pymongo.cursor import Cursor
from pymongo.database import Database
from pymongo.results import InsertOneResult, InsertManyResult

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

    def create_index(self, builder: 'IndexBuilder'):
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
            if not builder.one:
                logger.debug('db.%s.find(%s)', builder.collection._name,
                             builder.query_filer_document)
                return self.db()[builder.collection._name].find(
                    builder.query_filer_document)
            else:
                logger.debug('db.%s.find_one(%s)', builder.collection._name,
                             builder.query_filer_document)
                return self.db()[builder.collection._name].find_one(
                    builder.query_filer_document)
        elif isinstance(builder, InsertBuilder):
            if not builder.one:
                logger.debug('db.%s.insert_many(%s)', builder.collection._name,
                             [builder.documents[0], '...'] if len(
                                 builder.documents) > 1 else builder.documents)
                return self.db()[builder.collection._name].insert_many(
                    builder.documents, session=self.session())
            else:
                logger.debug('db.%s.insert_one(%s)', builder.collection._name,
                             builder.documents[0])
                return self.db()[builder.collection._name].insert_one(
                    builder.documents[0], session=self.session())
        elif isinstance(builder, UpdateBuilder):
            if not builder.one:
                logger.debug('db.%s.update(%s, %s)', builder.collection._name,
                             builder.filter_expression,
                             builder.update_operators)
                return self.db()[builder.collection._name].update_many(
                    builder.filter_expression, builder.update_operators,
                    upsert=builder.upsert, session=self.session())
            else:
                logger.debug('db.%s.update_one(%s, %s)',
                             builder.collection._name,
                             builder.filter_expression,
                             builder.update_operators)
                return self.db()[builder.collection._name].update_one(
                    builder.filter_expression, builder.update_operators,
                    upsert=builder.upsert,
                    session=self.session())
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


class Collection(object):
    """
    A collection object which hosts fields.
    Any attribute of this object is considered as a field
    with the corresponding name.
    """

    def __init__(self, collection: Union['Collection', str]):
        self._name = collection if isinstance(collection,
                                              str) else collection._name

    def get_field(self, item) -> 'Field':
        """
        Same as getting an attribute. Useful when attribute name is a variable
        or it conflicts with class members.
        @param item:
        @return:
        """
        return Field(item)

    def __getattr__(self, item) -> 'Field':
        return self.get_field(item)


"""
Since Collection is used only for keeping name, it might be more convenient 
to reference to collection simple as 'document' in the queries
"""
document = Collection('')


class IndexBuilder(object):
    """
    Index object builder, to execute create_index
    """

    def __init__(self, collection: Collection):
        self.collection = collection
        self.keys = []
        self.is_unique = False

    def asc(self, field: str) -> 'IndexBuilder':
        self.keys.append((field, pymongo.ASCENDING))
        return self

    def desc(self, field: str) -> 'IndexBuilder':
        self.keys.append((field, pymongo.DESCENDING))
        return self

    def unique(self) -> 'IndexBuilder':
        self.is_unique = True
        return self


def index(collection: Collection) -> IndexBuilder:
    return IndexBuilder(collection)


class Executable(object):
    """
    Common base for anything which can be executed on
    the database connection, such as inserts, queries,
    aggregations etc.
    """


class QueryBuilder(Executable):
    """
    Query object builder
    """

    def __init__(self, collection: Collection, one: bool = False):
        self.collection = collection
        self.one = one
        self.query_filer_document = {}

    def filter(self, expression: 'Expression') -> 'QueryBuilder':
        self.query_filer_document.update(expression.to_obj(Context.CRUD))
        return self

    def get_query_filter_document(self) -> dict:
        return self.query_filer_document


def query(collection: Collection) -> QueryBuilder:
    return QueryBuilder(collection)


def query_one(collection: Collection) -> QueryBuilder:
    return QueryBuilder(collection, True)


class Context(object):
    """
    Context which expressions are located in. E.g. when we use
    `collection.field == <value>` it can be inside query or inside aggregate,
    in both cases transformation to object is different.
    """
    CRUD = 0
    AGGREGATION = 1


class Expression(object):
    """
    An expression.

    https://docs.mongodb.com/manual/meta/aggregation-quick-reference
    /#aggregation-expressions
    """

    def to_obj(self, context: int = Context.CRUD):
        raise NotImplementedError('Must be implemented in subclasses')

    @staticmethod
    def express(expression, context=Context.CRUD):
        """
        Express expression in mongo syntax. The expression may be an
        Expression object, in this case to_obj() is called, it can be string,
        numeric, boolean or None literal, in this case it's expressed as is,
        or it can be a document literal, in this case $literal operator is used.
        """
        if isinstance(expression, Expression):
            return expression.to_obj(context)
        if isinstance(expression,
                      (str, int, float, bool)) or expression is None:
            return expression
        if isinstance(expression, Iterable):
            return list(Expression.express(item, context) for item in expression)
        return {'$literal': expression}

    def __eq__(self, other):
        return JointEqExpression(self, other)

    def __gt__(self, other):
        return JointGtExpression(self, other)

    def __ge__(self, other):
        return JointGteExpression(self, other)

    def __lt__(self, other):
        return JointLtExpression(self, other)

    def __le__(self, other):
        return JointLteExpression(self, other)

    def __ne__(self, other):
        return JointNeExpression(self, other)

    def __add__(self, other):
        return AddPipelineOperator(self, other)

    def __radd__(self, other):
        return AddPipelineOperator(other, self)

    def __sub__(self, other):
        return SubtractPipelineOperator(self, other)

    def __rsub__(self, other):
        return SubtractPipelineOperator(other, self)

    def __truediv__(self, other):
        return DividePipelineOperator(other, self)

    def __rtruediv__(self, other):
        return DividePipelineOperator(self, other)

    def __mod__(self, other):
        return ModPipelineOperator(self, other)

    def __rmod__(self, other):
        return ModPipelineOperator(other, self)

    def __mul__(self, other):
        return MultiplyPipelineOperator(self, other)

    def __rmul__(self, other):
        return MultiplyPipelineOperator(other, self)

    def __getitem__(self, item):
        return ArrayElemAtPipelineOperator(self, item)

    def __contains__(self, item):
        return InPipelineOperator(item, self)

    def in_(self, array: Iterable):
        return JointInExpression(self, array)

    def if_null(self, replacement: Any):
        return IfNullPipelineOperator(self, replacement)


class Field(Expression):
    """
    A field path expression
    """

    def __init__(self, name: str):
        self._name = name

    def to_obj(self, context: int = Context.CRUD):
        return '$' + self._name

    def not_in_(self, array: Iterable):
        return NinExpression(self, array)

    def exists(self):
        return ExistsExpression(self, True)

    def not_exists(self):
        return ExistsExpression(self, False)

    def get_field(self, item) -> 'Field':
        """
        Same as getting an attribute. Useful when attribute name is a variable
        or it conflicts with class members.
        @param item:
        @return:
        """
        return Field(self._name + '.' + item)

    def __getattr__(self, item) -> 'Field':
        return self.get_field(item)


class FieldExpression(Expression):
    """
    Common base of Expression which matches some field
    """

    def __init__(self, field: Field):
        self.field = field

    def get_operator_expression(self):
        raise NotImplementedError('Must be implemented in subclasses')

    def to_obj(self, context: int = Context.CRUD):
        return {self.field._name: self.get_operator_expression()}


class EqExpression(FieldExpression):
    """
    Matches values that are equal to a specified value
    """

    def __init__(self, field: Field, right):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$eq': self.right}


class GtExpression(FieldExpression):
    """
    Matches values that are greater than a specified value
    """

    def __init__(self, field: Field, right):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$gt': self.right}


class GteExpression(FieldExpression):
    """
    Matches values that are greater or equal than a specified value
    """

    def __init__(self, field: Field, right):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$gte': self.right}


class InExpression(FieldExpression):
    """
    Matches any of values specified in an array
    """

    def __init__(self, field: Field, right: Iterable):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$in': list(self.right)}


class LtExpression(FieldExpression):
    """
    Matches values that are less than specified value
    """

    def __init__(self, field: Field, right):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$lt': self.right}


class LteExpression(FieldExpression):
    """
    Matches values that are less or equal to a specified value
    """

    def __init__(self, field: Field, right):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$lte': self.right}


class NeExpression(FieldExpression):
    """
    Matches all values that are not equal to a specified value
    """

    def __init__(self, field: Field, right):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$ne': self.right}


class NinExpression(FieldExpression):
    """
    Matches none of the values specified in an array
    """

    def __init__(self, field: Field, right: Iterable):
        super().__init__(field)
        self.right = right

    def get_operator_expression(self):
        return {'$nin': list(self.right)}


class AndExpression(Expression):
    """
    Joins query clauses with a logical AND
    """

    def __init__(self, *args: Expression):
        self.expressions = args

    def to_obj(self, context: int = Context.CRUD):
        return {
            '$and': [expression.to_obj(context) for expression in
                     self.expressions]}


class NotExpression(FieldExpression):
    """
    Inverts the effect of query expression
    """

    def __init__(self, expression: FieldExpression):
        super().__init__(expression.field)
        self.expression = expression

    def get_operator_expression(self):
        return {'$not': self.expression.get_operator_expression()}


class NorExpression(Expression):
    """
    Joins query clauses with a logical NOR
    """

    def __init__(self, *args: Expression):
        self.expressions = args

    def to_obj(self, context: int = Context.CRUD):
        return {
            '$nor': [expression.to_obj(context) for expression in
                     self.expressions]}


class OrExpression(Expression):
    """
    Joins query clauses with a logical OR
    """

    def __init__(self, *args: Expression):
        self.expressions = args

    def to_obj(self, context: int = Context.CRUD):
        return {'$or': [expression.to_obj(context) for expression in
                        self.expressions]}


def and_(*args: Expression):
    return JointAndExpression(*args)


def not_(expression: Expression):
    return JointNotExpression(expression)


def nor_(*args: Expression):
    return NorExpression(*args)


def or_(*args: Expression):
    return JointOrExpression(*args)


class ExistsExpression(FieldExpression):
    """
    Matches documents that have a certain field
    """

    def __init__(self, field: Field, exists: bool):
        super().__init__(field)
        self.exists = exists

    def get_operator_expression(self):
        return {'$exists': self.exists}


class InsertBuilder(Executable):
    """
    Insert builder
    """

    def __init__(self, collection: Collection, one: bool, *args: dict):
        self.collection = collection
        self.one = one
        self.documents = args


def insert_one(collection: Collection, document: dict):
    return InsertBuilder(collection, True, *[document])


def insert_many(collection: Collection, documents: Iterable[dict]):
    return InsertBuilder(collection, False, *documents)


class UpdateBuilder(Executable):
    """
    Update builder
    """

    def __init__(self, collection: Collection, one: bool = False,
                 upsert: bool = False):
        self.collection = collection
        self.one = one
        self.upsert = upsert
        self.filter_expression = {}
        self.update_operators = {}

    def filter(self, expression: Expression) -> 'UpdateBuilder':
        self.filter_expression.update(expression.to_obj(Context.CRUD))
        return self

    def set(self, d: dict) -> 'UpdateBuilder':
        self.update_operators.update({'$set': d})
        return self

    def unset(self, *fields: str) -> 'UpdateBuilder':
        self.update_operators.update({'$unset': dict((field, '') for field in fields)})
        return self


def update(collection: Collection, upsert: bool = False):
    return UpdateBuilder(collection, upsert=upsert)


def update_one(collection: Collection, upsert: bool = False):
    return UpdateBuilder(collection, one=True, upsert=upsert)


def update_many(collection: Collection, upsert: bool = False):
    return UpdateBuilder(collection, one=False, upsert=upsert)


class DeleteBuilder(Executable):
    """
    Delete builder
    """

    def __init__(self, collection: Collection):
        self.collection = collection
        self.filter_expression = {}

    def filter(self, expression: Expression):
        self.filter_expression.update(expression.to_obj(Context.CRUD))


def delete(collection: Collection):
    return DeleteBuilder(collection)


class AggregationPipelineBuilder(Executable):
    """
    Aggregation pipeline builder
    """

    def __init__(self, collection: Collection = None):
        self.collection = collection
        self.stages = []

    def match(self, query: Expression) -> 'AggregationPipelineBuilder':
        self.stages.append(MatchPipelineStage(query))
        return self

    def add_fields(self, **kwargs) -> 'AggregationPipelineBuilder':
        self.stages.append(AddFieldsPipelineStage(**kwargs))
        return self

    def bucket(self, group_by: Expression,
               boundaries: Union[Iterable[int], Iterable[float]],
               default: Any,
               **kwargs: Expression) -> 'AggregationPipelineBuilder':
        self.stages.append(BucketPipelineStage(group_by, boundaries, default, **kwargs))
        return self

    def project(self, *args: Field, **kwargs) -> 'AggregationPipelineBuilder':
        self.stages.append(ProjectPipelineStage(*args, **kwargs))
        return self

    def group(self, _id,
              **kwargs: 'AccumulatorExpression') -> \
            'AggregationPipelineBuilder':
        self.stages.append(GroupPipelineStage(_id, **kwargs))
        return self

    def lookup(self, collection: Collection, local_field: Union[str, Field],
               foreign_field: Union[str, Field],
               as_: str) -> 'AggregationPipelineBuilder':
        self.stages.append(
            LookupPipelineStage(collection, local_field, foreign_field, as_))
        return self

    def replace_root(self,
                     new_root: Expression) -> 'AggregationPipelineBuilder':
        self.stages.append(ReplaceRootPipelineStage(new_root))
        return self

    def unset(self, *excluded_fields: Union[Field, str]) -> 'AggregationPipelineBuilder':
        self.stages.append(UnsetPipelineStage(*excluded_fields))
        return self

    def unwind(self, field: Field) -> 'AggregationPipelineBuilder':
        self.stages.append(UnwindPipelineStage(field))
        return self

    def sort(self, *args: Tuple[Field, int]) -> 'AggregationPipelineBuilder':
        self.stages.append(SortPipelineStage(*args))
        return self

    def facet(self, **kwargs: 'AggregationPipelineBuilder') -> 'AggregationPipelineBuilder':
        self.stages.append(FacetPipelineStage(**kwargs))
        return self

    def get_pipeline(self):
        return [stage.to_obj(Context.AGGREGATION) for stage in self.stages]


def aggregate(collection: Collection = None):
    return AggregationPipelineBuilder(collection)


class PipelineStage(Expression):
    """
    Expression that represents one pipeline stage
    """


class MatchPipelineStage(PipelineStage):
    """
    Filters document stream
    """

    def __init__(self, query: Expression):
        self.query = query

    def to_obj(self, context: int = Context.AGGREGATION):
        if isinstance(self.query, FieldExpression):
            return {'$match': self.query.to_obj(context)}

        return {'$match': {'$expr': self.query.to_obj(context)}}


class AddFieldsPipelineStage(PipelineStage):
    """
    Adds new fields to the document
    """

    def __init__(self, **kwargs: Any):
        self.new_fields = kwargs

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$addFields': dict(
            (name, Expression.express(expression, context)) for name, expression
            in
            self.new_fields.items())}


class BucketPipelineStage(PipelineStage):
    """
    Categorizes incoming documents into groups, called buckets,
     based on a specified expression and bucket boundaries and
     outputs a document per each bucket. Each output document
     contains an _id field whose value specifies the inclusive
     lower bound of the bucket.
    """

    def __init__(self, group_by: Expression,
                 boundaries: Union[Iterable[int], Iterable[float]],
                 default: Any,
                 **kwargs: Expression):
        self.group_by = group_by
        self.boundaries = boundaries
        self.default = default
        self.output = kwargs

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$bucket': {
                'groupBy': Expression.express(self.group_by, context),
                'boundaries': list(self.boundaries),
                'default': self.default,
                'output': dict((name, Expression.express(value, context))
                               for name, value in self.output.items())
            }
        }


class ProjectPipelineStage(PipelineStage):
    """
    Passes along the document with the requested fields
    """

    def __init__(self, *args: Field, **kwargs: Any):
        self.included_fields = args
        self.new_or_reset_fields = kwargs

    def to_obj(self, context: int = Context.AGGREGATION):
        spec = dict((field._name, 1) for field in self.included_fields)
        spec.update(
            dict((name, Expression.express(expression, context)) for
                 name, expression in
                 self.new_or_reset_fields.items()))
        return {'$project': spec}


class UnsetPipelineStage(PipelineStage):
    """
    Removes/excludes fields from documents
    """

    def __init__(self, *args: Union[Field, str]):
        self.excluded_fields = args

    def to_obj(self, context: int = Context.AGGREGATION):
        def field_name(field):
            if isinstance(field, Field):
                return field._name
            return field

        return {'$unset': [field_name(field) for field in self.excluded_fields]}


class UnwindPipelineStage(PipelineStage):
    """
    Deconstructs an array field from the input for *each*
    element. Each output document with the value of the array
    field replaced by the element.
    """

    def __init__(self, field: Field):
        self.field = field

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$unwind': Expression.express(self.field, context)}


class SortPipelineStage(PipelineStage):
    """
    Sorts input documents and returns them to the pipeline
    in sorted order
    """

    def __init__(self, *args: Tuple[Field, int]):
        self.fields = args

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$sort': dict((field[0]._name, field[1]) for field in self.fields)}


class GroupPipelineStage(PipelineStage):
    """
    Groups input documents by specified _id expression and
    for each distinct grouping, outputs a document
    """

    def __init__(self, _id: Union[Expression, None],
                 **kwargs: 'AccumulatorExpression'):
        self._id = _id
        self.new_fields = kwargs

    def to_obj(self, context: int = Context.AGGREGATION):
        spec = {'_id': Expression.express(self._id, context)}
        spec.update(
            dict((name, Expression.express(expression, context)) for
                 name, expression in
                 self.new_fields.items()))
        return {'$group': spec}


class FacetPipelineStage(PipelineStage):
    """
    Process multiple aggregation pipelines within a single stage
    on the same set of input documents, each sub-pipeline as its own field
    in the output document where its results are stored as an array
    of documents.
    """

    def __init__(self, **kwargs: AggregationPipelineBuilder):
        self.kw = kwargs

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$facet': dict((name, pipeline.get_pipeline())
                           for name, pipeline in self.kw.items())
        }


class AccumulatorExpression(Expression):
    def __init__(self, operator: str, expression: Any):
        self.operator = operator
        self.expression = expression

    def to_obj(self, context: int = Context.AGGREGATION):
        return {self.operator: Expression.express(self.expression, context)}


class UserDefinedAccumulatorExpression(Expression):
    """
    Defines a custom accumulator operator
    """

    def __init__(self, init, init_args: Optional[Iterable], accumulate,
                 accumulate_args: Iterable, merge, finalize, lang: str = 'js'):
        self.init = init
        self.init_args = init_args
        self.accumulate = accumulate
        self.accumulate_args = accumulate_args
        self.merge = merge
        self.finalize = finalize
        self.lang = lang

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            'init': self.init,
            'initArgs': list(self.init_args) if self.init_args else None,
            'accumulate': self.accumulate,
            'accumulateArgs': [Expression.express(arg, context) for arg in
                               self.accumulate_args],
            'merge': self.merge,
            'finalize': self.finalize,
            'lang': self.lang
        }


class PercentileExpression(Expression):
    def __init__(self, input: Any, p: Iterable[Any]):
        self.input = input
        self.p = p

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$percentile': {
                'input': Expression.express(self.input, context),
                'p': Expression.express(self.p, context),
                'method': 'approximate',
            }
        }


class MedianExpression(Expression):
    def __init__(self, input: Any):
        self.input = input

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$median': {
                'input': Expression.express(self.input, context),
                'method': 'approximate',
            }
        }


def sum_(expression):
    """
    Returns a sum of numerical values. Ignores non-numerical values
    """
    return AccumulatorExpression('$sum', expression)


def min_(expression):
    """
    Returns the lowest expression value for each group
    """
    return AccumulatorExpression('$min', expression)


def max_(expression):
    """
    Returns the highest expression value for each group
    """
    return AccumulatorExpression('$max', expression)


def avg(expression):
    """
    Returns the average of numerical values. Ignores non-numerical values
    """
    return AccumulatorExpression('$avg', expression)


def std(expression):
    """
    Calculates the population standard deviation of the input values
    """
    return AccumulatorExpression('$stdDevPop', expression)


def push_(expression):
    """
    Returns an array of expression values for each group
    """
    return AccumulatorExpression('$push', expression)


def accumulator(init, init_args: Optional[Iterable], accumulate,
                accumulate_args: Iterable, merge, finalize):
    """
    Return a result of a user-defined accumulation function
    @param init: Function to initialize state
    @param init_args: Arguments passed to init, can only be constants
    @param accumulate: Function to accumulate documents,
    the accumulate function receives current state and values of
    expression from accumulate_args
    @param accumulate_args: Arguments passed to accumulate
    @param merge: Function to merge two states, received state1 and state2
    @param finalize: Function to finalize state
    @return: User-defined accumulator expression
    """
    return AccumulatorExpression('$accumulator',
                                 UserDefinedAccumulatorExpression(init,
                                                                  init_args,
                                                                  accumulate,
                                                                  accumulate_args,
                                                                  merge,
                                                                  finalize))


def push_unique(expression):
    """
    Return an array of unique expression values for each group
    """
    return AccumulatorExpression('$addToSet', expression)


def first(expression):
    """
    Return value from the first document for each group
    """
    return AccumulatorExpression('$first', expression)


def last(expression):
    """
    Return value from the last document for each group
    """
    return AccumulatorExpression('$last', expression)


def percentile(input: Any, p: Iterable[Any]):
    return PercentileExpression(input, p)


def median(input: Any):
    """
    Return an approximation of the median, the 50% percentile, as a scalar value.
    """
    return MedianExpression(input)


class LookupPipelineStage(PipelineStage):
    """
    Perform "left outer join" to the another document in the same database,
    to filter in "joined" documents for processing.
    """

    def __init__(self, collection: Collection, local_field: Union[str, Field],
                 foreign_field: Union[str, Field], as_: str):
        self.collection = collection
        self.local_field = local_field if isinstance(local_field,
                                                     str) else local_field._name
        self.foreign_field = foreign_field if isinstance(foreign_field,
                                                         str) else \
            foreign_field._name
        self.as_ = as_

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$lookup': {
            'from': self.collection._name,
            'localField': self.local_field,
            'foreignField': self.foreign_field,
            'as': self.as_
        }}


class ReplaceRootPipelineStage(PipelineStage):
    """
    Replaces the input document with the specified document.
    """

    def __init__(self, new_root: Expression):
        self.new_root = new_root

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$replaceRoot': {'newRoot': self.new_root.to_obj(context)}}


class PipelineOperator(Expression):
    """
    Expression that represents a pipeline operator.
    """


class AddPipelineOperator(PipelineOperator):
    def __init__(self, *args: Any):
        self.expressions = args

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$add': [Expression.express(expression, context) for expression in
                     self.expressions]}


class DividePipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$divide': [Expression.express(self.left, context),
                        Expression.express(self.right, context)]}


class ModPipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$mod': [Expression.express(self.left, context),
                         Expression.express(self.right, context)]}


class MultiplyPipelineOperator(PipelineOperator):
    def __init__(self, *args: Any):
        self.expressions = args

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$multiply': [Expression.express(expression, context) for expression
                          in self.expressions]}


class SubtractPipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$subtract': [Expression.express(self.left, context),
                              Expression.express(self.right, context)]}


class ArrayElemAtPipelineOperator(PipelineOperator):
    def __init__(self, array: Any, idx: Any):
        self.array = array
        self.idx = idx

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$arrayElemAt': [Expression.express(self.array, context),
                                 Expression.express(self.idx, context)]}


class FilterPipelineOperator(PipelineOperator):
    def __init__(self, cond: Callable[[Field], Expression], array: Any):
        self.array = array
        self.cond = cond

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$filter': {'input': Expression.express(self.array, context),
                            'cond': Expression.express(
                                self.cond(Field('$this')),
                                context)}}


def filter_(cond: Callable[[Field], Expression], array: Any):
    return FilterPipelineOperator(cond, array)


class CondPipelineOperator(PipelineOperator):
    def __init__(self, if_expr: Expression, then_expr: Any, else_expr: Any):
        self.if_expr = if_expr
        self.then_expr = then_expr
        self.else_expr = else_expr

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$cond': {
                'if': Expression.express(self.if_expr, context),
                'then': Expression.express(self.then_expr, context),
                'else': Expression.express(self.else_expr, context),
            }
        }


def cond(if_expr: Expression, then_expr: Any, else_expr: Any):
    return CondPipelineOperator(if_expr, then_expr, else_expr)


class SwitchPipelineOperator(PipelineOperator):
    def __init__(self, branches: Iterable[Tuple[Any, Any]],
                 default: Any = None):
        self.branches = branches
        self.default = default

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$switch': {
            'branches': [{
                'case': Expression.express(case, context),
                'then': Expression.express(expr, context),
            } for case, expr in self.branches],
            'default': Expression.express(self.default, context),
        }}


def switch(branches: Iterable[Tuple[Any, Any]], default: Any = None):
    return SwitchPipelineOperator(branches, default)


class InPipelineOperator(PipelineOperator):
    def __init__(self, expression: Any, array: Any):
        self.expression = expression
        self.array = array

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$in': [Expression.express(self.expression, context),
                        Expression.express(self.array, context)]}


class JointInExpression(Expression):
    def __init__(self, expression: Any, array: Any):
        self.expression = expression
        self.array = array

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return InExpression(self.expression, self.array).to_obj(context)
        return InPipelineOperator(self.expression, self.array).to_obj(context)


class AndPipelineOperator(PipelineOperator):
    def __init__(self, *args: Any):
        self.expressions = args

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$and': [Expression.express(expression, context) for expression in
                     self.expressions]}


class JointAndExpression(Expression):
    """
    Joint class to handle both query and aggregation AND operators
    """

    def __init__(self, *args: Any):
        self.expressions = args

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return AndExpression(*self.expressions).to_obj(context)
        return AndPipelineOperator(*self.expressions).to_obj(context)


class NotPipelineOperator(PipelineOperator):
    def __init__(self, expression: Any):
        self.expression = expression

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$not': [Expression.express(self.expression, context)]}


class JointNotExpression(Expression):
    def __init__(self, expression: Any):
        self.expression = expression

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return NotExpression(self.expression).to_obj(context)
        return NotPipelineOperator(self.expression).to_obj(context)


class OrPipelineOperator(PipelineOperator):
    def __init__(self, *args: Any):
        self.expressions = args

    def to_obj(self, context: int = Context.AGGREGATION):
        return {
            '$or': [Expression.express(expression, context) for expression in
                    self.expressions]}


class JointOrExpression(Expression):
    def __init__(self, *args: Any):
        self.expressions = args

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return OrExpression(*self.expressions).to_obj(context)
        return OrPipelineOperator(*self.expressions).to_obj(context)


class CmpPipelineOperator(PipelineOperator):
    """
    Returns 0 if the two values are equivalent, 1 if the first value is
    greater than the second, and -1 if the first value os less than the second
    """

    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$cmp': [Expression.express(self.right, context),
                         Expression.express(self.right, context)]}


def cmp(left: Any, right: Any):
    return CmpPipelineOperator(left, right)


class EqPipelineOperator(PipelineOperator):
    """
    Returns true if the values are equal
    """

    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$eq': [Expression.express(self.left, context),
                        Expression.express(self.right, context)]}


class JointEqExpression(Expression):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return EqExpression(self.left, self.right).to_obj(context)
        return EqPipelineOperator(self.left, self.right).to_obj(context)


class GtPipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$gt': [Expression.express(self.left, context),
                        Expression.express(self.right, context)]}


class JointGtExpression(Expression):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return GtExpression(self.left, self.right).to_obj(context)
        return GtPipelineOperator(self.left, self.right).to_obj(context)


class GtePipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$gte': [Expression.express(self.left, context),
                         Expression.express(self.right, context)]}


class JointGteExpression(Expression):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return GteExpression(self.left, self.right).to_obj(context)
        return GtePipelineOperator(self.left, self.right).to_obj(context)


class LtPipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$lt': [Expression.express(self.left, context),
                        Expression.express(self.right, context)]}


class JointLtExpression(Expression):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return LtExpression(self.left, self.right).to_obj(context)
        return LtPipelineOperator(self.left, self.right).to_obj(context)


class LtePipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$lte': [Expression.express(self.left, context),
                         Expression.express(self.right, context)]}


class JointLteExpression(Expression):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return LteExpression(self.left, self.right).to_obj(context)
        return LtePipelineOperator(self.left, self.right).to_obj(context)


class NePipelineOperator(PipelineOperator):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$ne': [Expression.express(self.left, context),
                        Expression.express(self.right, context)]}


class JointNeExpression(Expression):
    def __init__(self, left: Any, right: Any):
        self.left = left
        self.right = right

    def to_obj(self, context: int = Context.CRUD):
        if context == Context.CRUD:
            return NeExpression(self.left, self.right).to_obj(context)
        return NePipelineOperator(self.left, self.right).to_obj(context)


class IfNullPipelineOperator(PipelineOperator):
    """
    Evaluates an expression and returns the value of the expression if the
    expression evaluates to a non-null value. If the expression evaluates
    to a null value, including instances of undefined values or missing fields,
    returns the value of the replacement expression.
    """

    def __init__(self, expression: Any, replacement: Any):
        self.expression = expression
        self.replacement = replacement

    def to_obj(self, context: int = Context.AGGREGATION):
        return {'$ifNull': [Expression.express(self.expression, context),
                            Expression.express(self.replacement, context)]}


class DictExpression(Expression):
    """
    Expression to construct a dict (document) inside a pipeline stage
    """

    def __init__(self, **kwargs: Any):
        self.items = kwargs

    def to_obj(self, context: int = Context.AGGREGATION):
        return dict((key, Expression.express(value, context)) for key, value in
                    self.items.items())


def dict_(**kwargs: Any):
    return DictExpression(**kwargs)


class ListExpression(Expression):
    """
    Expression to construct a list (array) inside a pipeline stage
    """

    def __init__(self, *args: Any):
        self.expressions = args

    def to_obj(self, context: int = Context.CRUD):
        return [Expression.express(expression, context) for expression in
                self.expressions]


def list_(*args: Any):
    return ListExpression(*args)
