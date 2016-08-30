import pandas as pd

from stores import SqlStore, HdfStore

STORES_MAP = {
    'sql': SqlStore,
    'hdf': HdfStore,
    # 'csv': pandas csv
    # 'excel': pandas csv
    # 'html': pandas csv
    # 'email'
}


class DataStore(object):

    def __init__(self):
        self._sources = {}

    def __getattr__(self, name):
        if self.exists(name):
            return self._sources[name]
        raise AttributeError("%r object has no attribute %r" %
                             (self.__class__, name))

    def exists(self, name):
        return self._sources.has_key(name)

    def add(self, name, source):
        if self.exists(name):
            raise Exception("Source {n} already exists".format(n=name))
        self._sources[name] = source

    def delete(self, name):
        del self._sources[name]

    def get(self, name):
        return self._sources[name]

    def get_data(self, name):
        return self.get(name).data

    def all(self):
        return self._sources

    def show(self, source, n=5):
        s = self.get_data(source)
        return s.head(n=n), s.shape


class Pipeline(object):
    """Object representing an sql operation

    Attributes:
        * `data`: loaded data
    """

    def __init__(self):
        self.data = None
        self.datastore = DataStore()

    def _todf(self, data, columns=None):
        """Transform iterable in a pandas dataframe.
        `data` must be an iterable
        """
        df = pd.DataFrame(data)
        df.columns = columns
        return df

    def show(self, n=5):
        return self.data.head(n=n), self.data.shape

    def add_source(self, store, name, stype, **kwargs):
        """Configure source data
        """
        store = STORES_MAP.get(store, None)
        if store is None:
            raise Exception("Invalid store")
        self.datastore.add(name, store(name, stype, **kwargs))
        return self.datastore.get(name)

    def del_source(self, name):
        self.datastore.delete(name)

    def extract(self, name, params=None, save=False):
        source = self.datastore.get(name)
        params = params or {}
        data = source.extract(**params)
        if save:
            self.data = data
        return data

    def clean(self, store, *args, **kwargs):
        """Cleans the target data
        """
        source = self.datastore.get(store)
        return source.clean(*args, **kwargs)

    def load(self, name, params=None):
        """Clean target, to prevent loading duplicated rows
        """
        target = self.datastore.get(name)
        params = params or {}
        target.load(self.data, **params)
        return len(self.data)

    def transform(self, store, func, *args, **kwargs):
        if store:
            return self.datastore.get(store).transform(func, *args, **kwargs)
        return func(self, *args, **kwargs)

    def add_column(self, name, data):
        self.data.loc[:, name] = data

    def to_datetime(self, columns):
        for column in columns:
            self.data[column] = pd.to_datetime(self.data[column])

    def concat(self, args, save=False):
        objs = [self.datastore.get_data(source) for source in args]
        data = pd.concat(objs)
        if save:
            self.data = data
        return data

    def update(self, store, *args, **kwargs):
        """Updates the target table with passed values
        """
        source = self.datastore.get(store)
        return source.update(*args, **kwargs)
