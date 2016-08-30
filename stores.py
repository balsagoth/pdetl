import os
import imp
import sqlalchemy
import pandas as pd

SOURCE_TYPES = ('source', 'target', 'staging')

try:
    imp.find_module('tables')
except ImportError:
    raise ImportError("Install pytables to suport HdfStore")


class Store(object):
    """Class where all stores should inherit
    """
    def load(self):
        """Load data to store
        """
        raise NotImplementedError

    def extract(self):
        """Extract data from store
        """
        raise NotImplementedError


class HdfStore(Store):

    def __init__(self, name, stype, path=None, filename=None, key="df"):
        # name of the store
        self.name = name
        self.stype = stype
        self.filename = filename or name
        self.path = path
        self.key = key
        self.data = None

    @property
    def fullpath(self):
        return os.path.join(self.path, self.filename)

    def exists(self):
        return os.path.exists(self.fullpath)

    def extract(self):
        if not self.exists():
            raise Exception("File does not exists")
        if self.data is None:
            self.data = pd.io.pytables.read_hdf(self.fullpath, self.key)
        return self.data

    def load(self, data, overwrite=False):
        if self.exists() and not overwrite:
            raise Exception("File already exists, use overwrite=True")
        self.data = data
        self.data.to_hdf(self.fullpath, self.key)


class SqlStore(Store):

    def __init__(self, name, stype, url=None, engine=None, schema=None,
                 options=None, table=None):

        if not url and not engine:
            raise AttributeError("Either url or connectable should be "
                                 "provided for SqlStore")

        if engine:
            self._engine = engine
        else:
            options = options or {}
            self._engine = sqlalchemy.create_engine(url, **options)

        # name of the source
        self.name = name
        self.schema = schema
        self.metadata = sqlalchemy.MetaData(bind=self._engine)

        if stype not in SOURCE_TYPES:
            raise Exception("Source type {s} needs to be one of "
                            "{ts}".format(s=stype, ts=SOURCE_TYPES))
        # source type. Could be source, target and staging
        self.stype = stype

        if stype in ("target") and table is None:
            raise Exception("Target and staging types must have table defined")
        # this is a table slqalchemy instance
        # maybe change to pandas SQLTable
        if table:
            self.table_name = table
            self.table = table

        # TODO
        # self.logger = get_logger()

    @property
    def table(self):
        if not isinstance(self._table, sqlalchemy.Table):
            raise Exception("Table must be a sqlalchemy Table instance")
        return self._table

    @table.setter
    def table(self, tablename):
        self._table = self.get_table(tablename)

    def tables(self, only=None):
        """Returns a list of the tables and views
        """
        self.metadata.reflect(schema=self.schema, views=True, only=only)
        return self.metadata.tables

    def get_table(self, name, schema=None, autoload=True):
        """Returns a table with `name`. If schema is not provided, then
        store's default is used.
        """
        if name is None:
            raise Exception("Table name should not be None")

        schema = schema or self.schema
        return sqlalchemy.Table(name, self.metadata, autoload=autoload,
                                schema=schema, autoload_with=self._engine)

    def _build_where(self, table, conditions):
        """conditions is a list of tuples (column, op, value)
        """
        filters = []
        for cond in conditions:
            c = table.columns[cond[0]]
            op = filter(lambda x: hasattr(c, x % cond[1]), ['%s', '%s_', '__%s__'])
            op = op[0] % cond[1]
            filters.append(getattr(c, op)(cond[2]))

        return filters

    def clean(self, conditions=None, binary_op="and"):
        bop = sqlalchemy.sql.and_ if binary_op == "and" else sqlalchemy.sql.or_
        where = self._build_where(self.table, conditions)
        delete = self.table.delete().where(bop(*where))
        return self._engine.execute(delete)

    def read_sql(self, query, params, **kwargs):
        # TODO implement logging
        # self.logger.debug("Execute SQL: %s" % str(query))
        return pd.io.sql.read_sql(query, self._engine, params=params, **kwargs)

    def _execute(self, query, *args, **kwargs):
        # TODO implement logging
        # self.logger.debug("Execute SQL: %s" % str(query))
        return self._engine.execute(query, *args, **kwargs)

    def extract(self, query=None, params=None, **options):
        if self.stype == "target":
            raise Exception("Cannot extract from target stype")
        self.data = self.read_sql(query, params, **options)
        return self.data

    def to_sql(self, data, table_name, if_exists="append", index=False, **options):
        options = options or {}
        pd.io.sql.to_sql(data, table_name, self._engine,
                         if_exists=if_exists, index=index, **options)
        return len(data)

    def load(self, data, **options):
        if (isinstance(data, pd.DataFrame) and data.empty):
            raise Exception("No data to load")
        if self.stype == "source":
            raise Exception("Cannot load to a source stype")
        return self.to_sql(data, self.table_name, **options)

    def transform(self, func, *args, **kwargs):
        return func(self, *args, **kwargs)

    def update(self, values=None, wherecolumn=None):
        metadata = sqlalchemy.MetaData(bind=self._engine)
        datatable = sqlalchemy.Table(self.table, metadata, autoload=True)
        if len(wherecolumn) == 1:
            update = sqlalchemy.sql.update(datatable)\
                .values(values)\
                .where(datatable.get_children(column_collections=True)[0] == wherecolumn)
        else:
            #TODO: develop whereclause for cases with more than 1 clause
            update = sqlalchemy.sql.update(datatable)\
                .values(values)\
                .where(sqlalchemy.and_(datatable.c.wherecolumn == wherecolumn))
        return self._engine.execute(update)
