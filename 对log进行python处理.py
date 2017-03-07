import re
import datetime
import threading
import queue
import time
from collections import namedtuple

regex = r'(?P<remote>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<time>.*)] "(?P<request>.*)" (?P<status>\d+) (?P<length>\d+) ".*" "(?P<ua>.*)'
matcher = re.compile(regex)
Request = namedtuple('Request', ['method', 'url', 'version'])
mapping = {
    'time': lambda x: datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z'),
    'request': lambda x: Request(*x.split()),
    'status': int,
    'length': int
}

def extract(line):
    m = matcher.match(line)
    if m:
        ret = m.groupdict()
        return {k:mapping.get(k, lambda x:x)(v) for k, v in ret.items()}
        # 把v的值作为参数，k在mapping中存在就取mapping[k]，否则用lambda
    raise Exception(line)

def read(f):
    for line in f:
        try:
            #yield extract(f.readline()) # other way
            yield extract(line)
        except:
            pass

def load(path): # read by stream
    with open(path) as f:
        while True:
            yield from read(f) # for x in read(f):yield x
            time.sleep(0.1)

def window(source: queue.Queue, handler, interval: int, width: int):
    store = [] # 时间间隔interval内存储
    start = None # set start time is None
    while True:
        #data = source.get() # 获取数据 # other way of queue
        data = next(source)
        store.append(data)
        current = data['time'] # save log current time
        if start is None:
            start = current # first time read
        if (current - start).total_seconds() >= interval: # 达到时间间隔
            start = current # 重置开始时间
            try:
                handler(store) # 处理这个时间间隔的数据
            except:
                pass
            dt = current - datetime.timedelta(seconds= width) # 记录这个时间间隔的开始时间
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
        #t = threading.Thread(target=window, args=(q, handler, interval, width)) # other way of queue
        analyers.append(t)

    def start():
        for t in analyers:
            t.start()
        for item in source:
            #print(item) # debug message
            for q in queues:
                q.put(item)

    return register, start

def null_handler(items):
    print('...')
    pass

def status_handler(items):
    print('---')
    status = {}
    for x in items:
        if x['status'] not in status.keys():
            status[x['status']] = 0
        status[x['status']] += 1
    total = sum(x for x in status.values())
    for k, v in status.items():
        print("{} => {}%".format(k, v/total * 100))

if __name__ == '__main__':
    import sys
    #register, start = dispatcher(load(sys.argv[1]))
    register, start = dispatcher(load('/home/Lock/PycharmProjects/log_analyzer/access.log'))
    #register(null_handler, 5, 10)
    register(status_handler, 5, 10)
    start()
