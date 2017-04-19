import re
import time
import queue
import datetime
import threading
from collections import namedtuple

matcher = re.compile(
    r'(?P<remote>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<time>.*)\] "(?P<request>.*)" (?P<status>\d+) (?P<length>\d+) ".*" "(?P<ua>.*)"')
Request = namedtuple('Request', ['method', 'url', 'version'])
mapping = {
    'length': int,
    'request': lambda x: Request(*x.split()),
    'status': int,
    'time': lambda x: datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z')
}


def extract(line):
    m = matcher.match(line)
    if m:
        ret = m.groupdict()
        return {k: mapping.get(k, lambda x: x)(v) for k, v in ret.items()}
    raise Exception(line)


def read(f):
    for line in f:
        try:
            yield extract(line)
        except:
            pass


def load(path):
    with open(path) as f:
        while True:
            yield from read(f)
            time.sleep(0.1)


def window(source, handler, interval: int, width: int):
    store = []
    start = None
    while True:
        data = next(source)
        store.append(data)
        current = data['time']
        if start is None:
            start = current
        if (current - start).total_seconds() >= interval:
            start = current
            try:
                handler(store)
            except:
                pass
            dt = current - datetime.timedelta(seconds=width)
            store = [x for x in store if x['time'] > dt]


def dispatcher(source):
    analyers = []
    queues = []

    def _source(q):
        while True:
            yield q.get()

    def register(handler, interval, width):
        q = queue.Queue()
        queues.append(q)
        t = threading.Thread(target=window, args=(_source(q), handler, interval, width))
        analyers.append(t)

    def start():
        for t in analyers:
            t.start()
        for item in source:
            #print(item)
            for q in queues:
                q.put(item)

    return register, start


# biz logic
def null_handler(items):
    pass


def status_handler(items):
    if len(items) <= 0:
        return
    print(items[0]['time'])
    status = {}
    for x in items:
        if x['status'] not in status.keys():
            status[x['status']] = 0
        status[x['status']] += 1
    total = sum(x for x in status.values())
    for k, v in status.items():
        print("\t{} => {:.2f}%".format(k, v / total * 100))
    print()


if __name__ == '__main__':
    import sys

    register, start = dispatcher(load('/home/Lock/PycharmProjects/log_analyzer/access.log'))
    register(null_handler, 5, 10)
    register(status_handler, 5, 10)
    start()
