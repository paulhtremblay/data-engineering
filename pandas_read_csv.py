import pandas
import tracemalloc
from collections import Counter
import linecache
import os
import csv

def display_top(snapshot, key_type='lineno', limit=3):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)

    print("Top %s lines" % limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        # replace "/path/to/module/file.py" with "module/file.py"
        filename = os.sep.join(frame.filename.split(os.sep)[-2:])
        print("#%s: %s:%s: %.1f KiB"
              % (index, filename, frame.lineno, stat.size / 1024))
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            print('    %s' % line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        print("%s other: %.1f KiB" % (len(other), size / 1024))
    total = sum(stat.size for stat in top_stats)
    print("Total allocated size: %.1f KiB" % (total / 1024))


def read_csv_pandas(path):
    df = pandas.read_csv(path)

def read_csv_native(path):
    with open(path, 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        for line in csv_reader:
            pass

def create_csv(path):

    data = [
            ['field1', 'field2',],
            ]
    with open(path, 'w') as write_obj:
        csv_writter = csv.writer(write_obj)
        csv_writter.writerows(data)
        for i in range(1000000):
            l = []
            for j in range(50):
                l.append(i + j)
            csv_writter.writerow(l)

if __name__ == '__main__':
    path = 'test.csv'
    create_csv(path)
    print('finished creating data')
    tracemalloc.start()
    read_csv_pandas(path = path )
    #read_csv_native(path = path)
    snapshot = tracemalloc.take_snapshot()
    display_top(snapshot)
