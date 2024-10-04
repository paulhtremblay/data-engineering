import os
import argparse
import pprint

pp = pprint.PrettyPrinter(indent= 4)

def _custom_type(s):
    if s > 0 and s < 1:
        return int(s)
    raise argparse.ArgumentTypeError('Value has to be between 0 and 1')



def _get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", '-v',  action ='store_true')  
    parser.add_argument("paths", nargs='+', help="path of file")
    parser.add_argument("--out", '-o',  required = True, help="out-path")
    parser.add_argument('--number', type = _custom_type)
    args = parser.parse_args()
    return args



if __name__== '__main__':
    args = _get_args()
    print(args)
