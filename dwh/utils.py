from collections import *
from typing import *
import datetime

def dispatch_function(func: Callable[..., Hashable]) -> Callable[..., Callable[..., Any]]:
    """
    Decorator that takes a dispatch function generating a key.
    The `register` function registers a specific function for a given key.
    """

    registry = {}

    def dispatch(key):
        if key not in registry:
            raise KeyError('Unknown dispatch key')
        return registry[key]

    def inner(*args, **kwargs):
        return dispatch(func(*args, **kwargs))(*args, **kwargs)

    def register(key, target=None):
        if target is None:
            return lambda f: register(key, f)
        registry[key] = target
        return target

    inner.register = register
    return inner


Node = namedtuple('Node', ('id', 'pid', 'tag', 'data', 'attrs', 'leaf'))


class XMLStack:
    """A SAX parser"""

    def __init__(self):
        self.count = 0
        self.stack = deque()
        self.pending = deque()

    def start(self, tag, attr):
        node = Node(self.count, None, tag, '', {**attr}, True)
        self.stack.append(node)
        self.count += 1

    def character(self, data):
        if len(self.stack) == 0:
            raise ValueError('Invalid structure')
        node = self.stack.pop()
        self.stack.append(node._replace(data=node.data + data))

    def end(self, tag):
        if len(self.stack) == 0:
            raise ValueError('Invalid structure')

        cnode = self.stack.pop()

        if cnode.tag != tag:
            raise ValueError('Invalid structure')

        if len(self.stack) > 0:
            pnode = self.stack.pop()

            if len(cnode.attrs) > 0 or not cnode.leaf:
                pnode = pnode._replace(leaf=False)
                self.pending.append((cnode.tag, {**cnode.attrs, '_nid': cnode.id, '_pid': pnode.id}))
            else:
                pnode = pnode._replace(attrs={**pnode.attrs, cnode.tag: cnode.data.strip()})

            self.stack.append(pnode)
        else:
            self.pending.append((cnode.tag, {**cnode.attrs, '_nid': cnode.id, '_pid': cnode.pid}))

    def items(self):
        yield from self.pending

    def clear(self):
        self.pending.clear()


class PersistentBuffer:
    """A context manager to log and persist pipelines operations"""

    class AlreadyProcessed(Exception):
        """Exit out of the with statement without processing"""

    class AlreadyProcessing(Exception):
        """Exit out of the with statement without processing"""

    def __init__(self, key, database, logs_table='logs', batch_size=1000):
        self.file = self.filename(key)
        self.database = database
        self.logs = logs_table
        self.batch_size = batch_size

    def __enter__(self):
        self.buffer = defaultdict(list)
        for completed in self.database[self.logs].find(source=self.file,status='completed'):
            raise self.AlreadyProcessed
        for started in self.database[self.logs].find(source=self.file,status='started'):
            raise self.AlreadyProcessing
        self.batch_id = self.database[self.logs].insert({'source': self.file, 'status': 'started', 'last_update_dt': datetime.datetime.now()})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type not in [self.AlreadyProcessed, self.AlreadyProcessing]:
            logs = self.database.load_table(self.logs)
            if exc_type is None:
                self.flush()
                logs.update({'batch_id': self.batch_id, 'source': self.file, 'status': 'completed', 'last_update_dt': datetime.datetime.now()}, ['batch_id'])
            else:
                logs.update({'batch_id': self.batch_id, 'source': self.file, 'status': 'failed', 'last_update_dt': datetime.datetime.now()}, ['batch_id'])


    def filename(self, key):
        return key[key.rfind('/')+1:]

    def add(self, table, row):
        self.buffer[table].append({**row, 'batch_id': self.batch_id})
        if len(self.buffer[table]) > self.batch_size:
            self.flush(table)

    def flush(self, table=None):
        if table is None:
            for table, rows in self.buffer.items():
                target = self.database.load_table(table)
                target.insert_many(rows, chunk_size=self.batch_size)
                rows.clear()
        else:
            target = self.database.load_table(table)
            target.insert_many(self.buffer[table], chunk_size=self.batch_size)
            self.buffer[table].clear()
