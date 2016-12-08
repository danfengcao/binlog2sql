#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, sys, argparse
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent

def command_line_parser():
    """Returns a command line parser used for binlog2sql"""

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
    range.add_argument('--start-pos', dest='startPos', type=int,
                       help='start position of start binlog file', default=4)
    range.add_argument('--end-file', dest='endFile', type=str,
                       help="End binlog file to be parsed. default: '--start-file'", default='')
    range.add_argument('--end-pos', dest='endPos', type=int,
                       help="stop position of end binlog file. default: end position of '--end-file'", default=0)
    parser.add_argument('--stop-never', dest='stopnever', action='store_true',
                        help='Wait for more data from the server. default: stop replicate at the last binlog when you start binlog2sql', default=False)

    parser.add_argument('--help', dest='help', action='store_true', help='help infomation', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    exclusive = parser.add_mutually_exclusive_group()
    exclusive.add_argument('--popPk', dest='popPk', action='store_true',
                           help='Generate insert sql without primary key if exists', default=False)
    exclusive.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                           help='Flashback data to start_postition of start_file', default=False)
    return parser

def command_line_args():
    parser = command_line_parser()
    args = parser.parse_args()
    if args.help:
        parser.print_help()
        sys.exit(1)
    if args.flashback and args.stopnever:
        raise ValueError('only one of flashback or stop-never can be True')
    if args.flashback and args.popPk:
        raise ValueError('only one of flashback or popPk can be True')
    return args


def compare_items((k, v)):
    #caution: if v is NULL, may need to process
    return '`%s`=%%s'%k

def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

def concat_sql_from_binlogevent(cursor, binlogevent, row=None, eStartPos=None, flashback=False, popPk=False):
    if flashback and popPk:
        raise ValueError('only one of flashback or popPk can be True')
    if type(binlogevent) not in (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, QueryEvent):
        raise ValueError('binlogevent must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')

    sql = ''
    if flashback is True:
        if isinstance(binlogevent, WriteRowsEvent):
            template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            sql = cursor.mogrify(template, map(fix_object, row['values'].values()))
        elif isinstance(binlogevent, DeleteRowsEvent):
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            sql = cursor.mogrify(template, map(fix_object, row['values'].values()))
        elif isinstance(binlogevent, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(['`%s`=%%s'%k for k in row['before_values'].keys()]),
                ' AND '.join(map(compare_items, row['after_values'].items())))
            sql = cursor.mogrify(template, map(fix_object, row['before_values'].values()+row['after_values'].values()))
    else:
        if isinstance(binlogevent, WriteRowsEvent):
            if popPk:
                tableInfo = (binlogevent.table_map)[binlogevent.table_id]
                if tableInfo.primary_key:
                    row['values'].pop(tableInfo.primary_key)
            template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            sql = cursor.mogrify(template, map(fix_object, row['values'].values()))
        elif isinstance(binlogevent, DeleteRowsEvent):
            template ='DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            sql = cursor.mogrify(template, map(fix_object, row['values'].values()))
        elif isinstance(binlogevent, UpdateRowsEvent):
            template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                binlogevent.schema, binlogevent.table,
                ', '.join(['`%s`=%%s'%k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            sql = cursor.mogrify(template, map(fix_object, row['after_values'].values()+row['before_values'].values()))
        elif isinstance(binlogevent, QueryEvent) and binlogevent.query != 'BEGIN' and binlogevent.query != 'COMMIT':
            sql ='USE {0};\n{1};'.format(
                binlogevent.schema, fix_object(binlogevent.query)
            )
    if type(binlogevent) in (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent):
        sql += ' #start %s end %s' % (eStartPos, binlogevent.packet.log_pos)
    return sql


class Binlog2sql(object):

    def __init__(self, connectionSettings, startFile=None, startPos=None, endFile=None,
                 endPos=None, only_schemas=None, only_tables=None, popPk=False, flashback=False, stopnever=False):
        '''
        connectionSettings: {'host': 127.0.0.1, 'port': 3306, 'user': slave, 'passwd': slave}
        '''
        if not startFile:
            raise ValueError('lack of parameter,startFile.')

        self.connectionSettings = connectionSettings
        self.startFile = startFile
        self.startPos = startPos if startPos else 4
        self.endFile = endFile if endFile else startFile
        self.endPos = endPos

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.popPk = popPk
        self.flashback = flashback
        self.stopnever = stopnever

        self.binlogList = []
        self.connection = pymysql.connect(**self.connectionSettings)
        try:
            cur = self.connection.cursor()
            cur.execute("SHOW MASTER STATUS")
            self.eofFile, self.eofPos = cur.fetchone()[:2]
            # if endFile and endPos:
            #     self.endFile, self.endPos = (endFile, endPos)
            # else:
            #     self.endFile, self.endPos = (self.eofFile, self.eofPos)

            cur.execute("SHOW MASTER LOGS")
            binIndex = [row[0] for row in cur.fetchall()]
            binlog2i = lambda x: x.split('.')[1]
            for bin in binIndex:
                if binlog2i(bin) >= binlog2i(self.startFile) and binlog2i(bin) <= binlog2i(self.endFile):
                    self.binlogList.append(bin)

            cur.execute("SELECT @@server_id")
            self.serverId = cur.fetchone()[0]
            if not self.serverId:
                raise ValueError('need set server_id in mysql server %s:%s' % (self.connectionSettings['host'], self.connectionSettings['port']))
        finally:
            cur.close()

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.connectionSettings, server_id=self.serverId,
                                    log_file=self.startFile, log_pos=self.startPos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True)

        cur = self.connection.cursor()
        tmpFile = 'tmp.%s.%s.tmp' % (self.connectionSettings['host'],self.connectionSettings['port']) # to simplify code, we do not use file lock for tmpFile.
        ftmp = open(tmpFile ,"w")
        flagLastEvent = False
        eStartPos = stream.log_pos
        lastPos = stream.log_pos
        try:
            for binlogevent in stream:
                if not self.stopnever:
                    if (stream.log_file == self.endFile and stream.log_pos == self.endPos) or (stream.log_file == self.eofFile and stream.log_pos == self.eofPos):
                        flagLastEvent = True
                    elif stream.log_file not in self.binlogList:
                        break
                    elif (self.endPos and stream.log_file == self.endFile and stream.log_pos > self.endPos) or (stream.log_file == self.eofFile and stream.log_pos > self.eofPos):
                        break
                    # else:
                    #     raise ValueError('unknown binlog file or position')

                if isinstance(binlogevent, QueryEvent) and binlogevent.query == 'BEGIN':
                    eStartPos = lastPos

                if isinstance(binlogevent, QueryEvent):
                    sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, flashback=self.flashback, popPk=self.popPk)
                    if sql:
                        print sql
                elif type(binlogevent) in (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent):
                    for row in binlogevent.rows:
                        sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, row=row , flashback=self.flashback, popPk=self.popPk, eStartPos=eStartPos)
                        if self.flashback:
                            ftmp.write(sql + '\n')
                        else:
                            print sql

                if type(binlogevent) not in (RotateEvent, FormatDescriptionEvent):
                    lastPos = binlogevent.packet.log_pos
                if flagLastEvent:
                    break
            ftmp.close()
            if self.flashback:
                # doesn't work if you can't fit the whole file in memory.
                # need to be optimized
                for line in reversed(open(tmpFile).readlines()):
                    print line.rstrip()
        finally:
            os.remove(tmpFile)
        cur.close()
        stream.close()
        return True

    def __del__(self):
        pass


if __name__ == '__main__':

    args = command_line_args()
    connectionSettings = {'host':args.host, 'port':args.port, 'user':args.user, 'passwd':args.password}
    binlog2sql = Binlog2sql(connectionSettings=connectionSettings, startFile=args.startFile,
                            startPos=args.startPos, endFile=args.endFile, endPos=args.endPos,
                            only_schemas=args.databases, only_tables=args.tables, popPk=args.popPk, flashback=args.flashback, stopnever=args.stopnever)
    binlog2sql.process_binlog()
