import os
import argparse
import pprint

pp = pprint.PrettyPrinter(indent= 4)


def _get_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("paths", nargs='+', help="path of file")
    parser.add_argument("--out", '-o',  required = True, help="out-path")


    args = parser.parse_args()

    return args



if __name__== '__main__':
    args = _get_args()
    print(args)
