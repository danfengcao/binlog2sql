#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import sys
from binlog2sql_util import is_valid_datetime

def parse_args(args):
    """parse args for binlog2sql"""
    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h','--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str,
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    range = parser.add_argument_group('range filter')
    range.add_argument('--start-position', '--start-pos', dest='startPos', type=int,
                       help='Start position of the --start-file', default=4)
    range.add_argument('--stop-position', '--end-pos', dest='endPos', type=int,
                       help="Stop position of --stop-file. default: latest position of '--stop-file'", default=0)
    range.add_argument('--start-datetime', dest='startTime', type=str,
                       help="Start reading the binlog at first event having a datetime equal or posterior to the argument; the argument must be a date and time in the local time zone, in any format accepted by the MySQL server for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 (you should probably use quotes for your shell to set it properly).", default='')
    range.add_argument('--stop-datetime', dest='stopTime', type=str,
                       help="Stop reading the binlog at first event having a datetime equal or posterior to the argument; the argument must be a date and time in the local time zone, in any format accepted by the MySQL server for DATETIME and TIMESTAMP types, for example: 2004-12-25 11:25:56 (you should probably use quotes for your shell to set it properly).", default='')
    parser.add_argument('--stop-never', dest='stopnever', action='store_true',
                        help='Wait for more data from the server. default: stop replicate at the last binlog when you start binlog2sql', default=False)

    parser.add_argument('--help', dest='help', action='store_true', help='help infomation', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')
    parser.add_argument('-f', '--file-path', dest='file_path', type=str, nargs='*', help='binlog file path', default='')

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='nopk', action='store_true',
                           help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                           help='Flashback data to start_postition of start_file', default=False)
    return parser

def command_line_args(args):
    needPrintHelp = False if args else True
    parser = parse_args(args)
    args = parser.parse_args(args)
    if args.help or needPrintHelp:
        parser.print_help()
        sys.exit(1)
    if args.flashback and args.stopnever:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.nopk:
        raise ValueError('Only one of flashback or nopk can be True')
    if (args.startTime and not is_valid_datetime(args.startTime)) or (args.stopTime and not is_valid_datetime(args.stopTime)):
        raise ValueError('Incorrect datetime argument')
    return args