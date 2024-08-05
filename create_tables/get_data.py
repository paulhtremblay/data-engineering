import time
import datetime
import os
from google.cloud import bigquery
from schemas import tv_streaming

import common
import get_sql
from prettytable import PrettyTable


def main():
    table = PrettyTable()
    client = bigquery.Client()

    sql = get_sql.get_sql_string(path = 'sql/group_streaming.sql')

    result=  client.query(sql)
    table = PrettyTable()

    for counter, i in enumerate(result):
        if counter == 0:
            table.field_names = list(i.keys())
        table.add_row(list(i.values()))
    print(table)

if __name__ == '__main__':
    #'/home/henry/Downloads/paul-henry-tremblay-f45a9e9028d5.json'
    key_path = os.path.join('/', 'home', 
    'henry', 'Downloads', 'paul-henry-tremblay-f7082df82497.json')
    assert os.path.isfile(key_path)

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] =  key_path
    main()
