import os
import argparse
import pprint

pp = pprint.PrettyPrinter(indent= 4)


def _get_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands',
                                   description='valid subcommands',
                                   help='additional help')

    parser_combine_files = subparsers.add_parser('combine-files', 
            help='combine lines from mult files into one or more lines in one file')
    parser_combine_files.add_argument("--verbose", '-v',  action ='store_true')  
    parser_combine_files.set_defaults(func=combine_files)
    parser_combine_files.add_argument("paths", nargs='+', help="path of file")
    parser_combine_files.add_argument("--out", '-o',  required = True, help="out-path")

    parser_convert_gpx = subparsers.add_parser('convert-to-gpx', help='convert to gpx')
    parser_convert_gpx.set_defaults(func=convert_to_gpx)
    parser_convert_gpx.add_argument("path", help="path of file")
    parser_convert_gpx.add_argument("--verbose", '-v',  action ='store_true')  
    parser_convert_gpx.add_argument("--out", "-o", required = False,  
            help="out path of file")


    args = parser.parse_args()
    args.func(args)

    return args


def combine_files(args):
    print(args)

def combine(args):
    print(args)

def convert_to_gpx(args):
    print(args)


if __name__== '__main__':
    args = _get_args()
    print(args)
