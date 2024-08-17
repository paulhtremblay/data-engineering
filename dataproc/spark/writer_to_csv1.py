import datetime
import csv
import uuid
import os

def main(in_dir = 'data', n = 2):
    with open(os.path.join(in_dir, f'{str(uuid.uuid1())}.csv'), 'w') as write_obj:
        csv_writer  = csv.writer(write_obj)
        for i in range(n):
            row = [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    1,1
                    ]
            csv_writer.writerow(row)

if __name__ == '__main__':
    main()
