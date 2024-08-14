import csv
import random
import datetime
import os
import uuid

def make_data(data_dir, num_rows = 3):
    now = datetime.datetime.now()
    with open(os.path.join(data_dir, f'{uuid.uuid1()}.csv'), 'w') as write_obj:
        csv_writer = csv.writer(write_obj)
        for i in range(num_rows):
            row = [now.strftime('%Y-%m-%d %H:%M:%S'), random.randint(1,10), random.randint(1, 10)]
            csv_writer.writerow(row)

if __name__ == '__main__':
    make_data(
            data_dir = 'data',
            num_rows = 3
            )
