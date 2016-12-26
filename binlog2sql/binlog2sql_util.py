#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, sys, argparse, datetime
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent


def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False

def create_unique_file(filename):
    version = 0
    resultFile = filename
    # if we have to try more than 1000 times, something is seriously wrong
    while os.path.exists(resultFile) and version<1000:
        resultFile = filename + '.' + str(version)
        version += 1
    if version >= 1000:
        raise OSError('cannot create unique file %s.[0-1000]' % filename)
    return resultFile

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
    range.add_argument('--start-file', dest='startFile', type=str,
                       help='Start binlog file to be parsed')
    range.add_argument('--start-position', '--start-pos', dest='startPos', type=int,
                       help='Start position of the --start-file', default=4)
    range.add_argument('--stop-file', '--end-file', dest='endFile', type=str,
                       help="Stop binlog file to be parsed. default: '--start-file'", default='')
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

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='nopk', action='store_true',
                           help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                           help='Flashback data to start_postition of start_file', default=False)
    return parser.parse_args(args)

def command_line_args(args):
    args = parse_args(args)
    if args.help:
        parser.print_help()
        sys.exit(1)
    if not args.startFile:
        raise ValueError('Lack of parameter: startFile')
    if args.flashback and args.stopnever:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.nopk:
        raise ValueError('Only one of flashback or nopk can be True')
    if (args.startTime and not is_valid_datetime(args.startTime)) or (args.stopTime and not is_valid_datetime(args.stopTime)):
        raise ValueError('Incorrect datetime argument')
    return args


def compare_items((k, v)):
    #caution: if v is NULL, may need to process
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k

def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

def concat_sql_from_binlogevent(cursor, binlogevent, row=None, eStartPos=None, flashback=False, nopk=False):
    if flashback and nopk:
        raise ValueError('only one of flashback or nopk can be True')
    if not (isinstance(binlogevent, WriteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, DeleteRowsEvent) or isinstance(binlogevent, QueryEvent)):
        raise ValueError('binlogevent must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    if isinstance(binlogevent, WriteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, DeleteRowsEvent):
        pattern = generate_sql_pattern(binlogevent, row=row, flashback=flashback, nopk=nopk)
        sql = cursor.mogrify(pattern['template'], pattern['values'])
        sql += ' #start %s end %s time %s' % (eStartPos, binlogevent.packet.log_pos, datetime.datetime.fromtimestamp(binlogevent.timestamp))
    elif flashback is False and isinstance(binlogevent, QueryEvent) and binlogevent.query != 'BEGIN' and binlogevent.query != 'COMMIT':
        if binlogevent.schema:
            sql = 'USE {0};\n'.format(binlogevent.schema)
        sql += '{0};'.format(fix_object(binlogevent.query))

    return sql

def generate_sql_pattern(binlogevent, row=None, flashback=False, nopk=False):
    template = ''
    values = []
    if flashback is True:
        if isinstance(binlogevent, WriteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlogevent, DeleteRowsEvent):
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlogevent, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(['`%s`=%%s'%k for k in row['before_values'].keys()]),
                ' AND '.join(map(compare_items, row['after_values'].items())))
            values = map(fix_object, row['before_values'].values()+row['after_values'].values())
    else:
        if isinstance(binlogevent, WriteRowsEvent):
            if nopk:
                # print binlogevent.__dict__
                # tableInfo = (binlogevent.table_map)[binlogevent.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlogevent.primary_key:
                    row['values'].pop(binlogevent.primary_key)

            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlogevent, DeleteRowsEvent):
            template ='DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlogevent, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(['`%s`=%%s'%k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            values = map(fix_object, row['after_values'].values()+row['before_values'].values())

    return {'template':template, 'values':values}
