#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, argparse
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent
)

def command_line_parser():
    """Returns a command line parser used for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to standard SQL')
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str,
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    position = parser.add_argument_group('position filter')
    position.add_argument('--stBinFile', dest='stBinFile', type=str, required=True,
                          help='Start binlog file to be parsed')
    position.add_argument('--stBinStPos', dest='stBinStPos', type=int,
                          help='start position of start binlog file', default=4)
    position.add_argument('--enBinFile', dest='enBinFile', type=str,
                          help='End binlog file to be parsed', default='')
    position.add_argument('--enBinEnPos', dest='enBinEnPos', type=int,
                          help='stop position of end binlog file', default=4)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='db you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='table you want to process', default='')

    exclusive = parser.add_mutually_exclusive_group()
    exclusive.add_argument('--popPk', dest='popPk', action='store_true',
                           help='Generate insert sql without primary key if exists', default=False)
    exclusive.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                           help='Flashback data to start_postition of start_file', default=False)
    return parser


def compare_items((k, v)):
    return '`%s`=%%s'%k

def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value

def concat_sql_from_binlogevent(cursor, binlogevent, row , flashback=False, popPk=False):
    if flashback and popPk:
        raise ValueError('only one of flashback or popPk can be True')
    if type(binlogevent) not in (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent):
        raise ValueError('binlogevent must be WriteRowsEvent, UpdateRowsEvent or DeleteRowsEvent')

    sql = ''
    if flashback is True:
        if isinstance(binlogevent, WriteRowsEvent):
            template = 'DELETE FROM {0} WHERE {1} LIMIT 1;'.format(
                binlogevent.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            sql = cursor.mogrify(template, row['values'].values())
        elif isinstance(binlogevent, DeleteRowsEvent):
            template = 'INSERT INTO {0}({1}) VALUES ({2});'.format(
                binlogevent.table,
                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            sql = cursor.mogrify(template, row['values'].values())
        elif isinstance(binlogevent, UpdateRowsEvent):
            template = 'UPDATE {0} SET {1} WHERE {2} LIMIT 1;'.format(
                binlogevent.table,
                ', '.join(['`%s`=%%s'%k for k in row['before_values'].keys()]),
                ' AND '.join(map(compare_items, row['after_values'].items())))
            sql = cursor.mogrify(template, row['before_values'].values()+row['after_values'].values())
    else:
        if isinstance(binlogevent, WriteRowsEvent):
            if popPk:
                tableInfo = (binlogevent.table_map)[binlogevent.table_id]
                if tableInfo.primary_key:
                    row['values'].pop(tableInfo.primary_key)
            template = 'INSERT INTO {0}({1}) VALUES ({2});'.format(
                binlogevent.table,
                ', '.join(map(lambda k: '`%s`'%k, row['values'].keys())),
                ', '.join(['%s'] * len(row['values']))
            )
            sql = cursor.mogrify(template, row['values'].values())
        elif isinstance(binlogevent, DeleteRowsEvent):
            template ='DELETE FROM {0} WHERE {1} LIMIT 1;'.format(
                binlogevent.table,
                ' AND '.join(map(compare_items, row['values'].items()))
            )
            sql = cursor.mogrify(template, row['values'].values())
        elif isinstance(binlogevent, UpdateRowsEvent):
            template = 'UPDATE {0} SET {1} WHERE {2} LIMIT 1;'.format(
                binlogevent.table,
                ', '.join(['`%s`=%%s'%k for k in row['after_values'].keys()]),
                ' AND '.join(map(compare_items, row['before_values'].items()))
            )
            sql = cursor.mogrify(template, row['after_values'].values()+row['before_values'].values())
    return sql


class Binlog2sql(object):

    def __init__(self, connectionSettings, stBinFile=None, stBinStPos=None, enBinFile=None,
                 enBinEnPos=None, only_schemas=[], only_tables=[], popPk=False, flashback=False):
        '''
        connectionSettings: {'host': 127.0.0.1, 'port': 3306, 'user': slave, 'passwd': slave}
        '''
        self.connectionSettings = connectionSettings
        self.stBinFile = stBinFile
        self.stBinStPos = stBinStPos
        if not stBinFile:
            raise ValueError('lack of parameter,stBinFile.')
        if not stBinStPos:
            self.stBinFile = 4

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.popPk = popPk
        self.flashback = flashback

        self.binlogList = []
        self.connection = pymysql.connect(**self.connectionSettings)
        try:
            cur = self.connection.cursor()
            cur.execute("SHOW MASTER STATUS")
            self.eofFile, self.eofPos = cur.fetchone()[:2]
            if enBinFile and enBinEnPos:
                self.enBinFile, self.enBinEnPos = (enBinFile, enBinEnPos)
            else:
                self.enBinFile, self.enBinEnPos = (self.eofFile, self.eofPos)

            cur.execute("SHOW MASTER LOGS")
            binIndex = [row[0] for row in cur.fetchall()]
            binlog2i = lambda x: x.split('.')[1]
            for bin in binIndex:
                if binlog2i(bin) >= binlog2i(self.stBinFile) and binlog2i(bin) <= binlog2i(self.enBinFile):
                    self.binlogList.append(bin)

            cur.execute("SELECT @@server_id")
            self.serverId = cur.fetchone()[0]
            if not self.serverId:
                raise ValueError('need set server_id in mysql server %s:%s' % (self.connectionSettings['host'], self.connectionSettings['port']))
        finally:
            cur.close()

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.connectionSettings, server_id=self.serverId,
                                    log_file=self.stBinFile, log_pos=self.stBinStPos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True)

        cur = self.connection.cursor()
        tmpFile = 'tmp.%s.%s.tmp' % (self.connectionSettings['host'],self.connectionSettings['port']) # to simplify code, we do not use file lock for tmpFile.
        ftmp = open(tmpFile ,"w")
        try:
            for binlogevent in stream:
                if (stream.log_file == self.enBinFile and stream.log_pos >= self.enBinEnPos) or (stream.log_file == self.eofFile and stream.log_pos >= self.eofPos):
                    break
                if type(binlogevent) not in (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent):
                    continue
                for row in binlogevent.rows:
                    sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, row=row , flashback=self.flashback, popPk=self.popPk)
                    if self.flashback:
                        ftmp.write(sql + '\n')
                    else:
                        print sql

            if self.flashback:
                # doesn't work if you can't fit the whole file in memory.
                # need to be optimized
                for line in reversed(open(tmpFile).readlines()):
                    print line.rstrip()

        finally:
            ftmp.close()
            os.remove(tmpFile)

        cur.close()
        stream.close()
        return True

    def __del__(self):
        pass


if __name__ == '__main__':

    parser = command_line_parser()
    args = parser.parse_args()
    connectionSettings = {'host':args.host, 'port':args.port, 'user':args.user, 'passwd':args.password}
    binlog2sql = Binlog2sql(connectionSettings=connectionSettings, stBinFile=args.stBinFile,
                            stBinStPos=args.stBinStPos, enBinFile=args.enBinFile, enBinEnPos=args.enBinEnPos,
                            only_schemas=args.databases, only_tables=args.tables, popPk=args.popPk, flashback=args.flashback)
    binlog2sql.process_binlog()
