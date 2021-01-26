# Motivation

This lib is intended to be some surrogate of
SqlAlchemy for no-sql database, particularly for
mongo.

I checked up MongoAlchemy (seems to be dead),
MongoEngine and some smaller libs but seems no one
of them fits.

Basically I need builders for query filters and
aggregation pipelines. However schema is not needed,
so any table can have any fields and any operation
can be applied to any field.

For example, instead of
```python
cursor = db.my_table.aggregate([{
    '$match': {
        '$expr': {
            '$and': [{
                '$eq': ['$col', 'col value']
            }, {
                '$in': [
                    '$details.key',
                    ['key 1', 'key 2', 'key 3']
                ]
            }]
        }
    },
}, ...])
```
I wish to write
```python
p = aggregate(my_table)
    .match(my_table.col == 'col value' and \
        my_table.details.key in ['key 1', 'key 2', 'key 3'])
    ....
cursor = conn.execute(p)
```

Advantages:
- more clear and readable code;
- can use IDE's autocomplete;
- ability to implement shortcuts for commonly used
operations.

## Naming

Because names `MongoAlchemy` and `NoSqlAlchemy` were
not vacant, I chose `mongomoron` because it was first
what came to mind, and because of that lib is for
morons like me for whom dealing with mongo's json
is too difficult.
