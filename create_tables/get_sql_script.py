import get_sql
import argparse

def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("path",  help="path of file")
    args = parser.parse_args()
    return args

def main(path):
    sql = get_sql.get_sql_string(path = path)
    print(sql)

if __name__ == '__main__':
    args = _get_args()
    main(path = args.path)

