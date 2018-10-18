#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, datetime
import pymysql
from binlogfile import BinLogFileReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import concat_sql_from_binlogevent, create_unique_file, reversed_lines
from bin2sql_util import command_line_args

class Bin2sql(object):
    def __init__(self, filePath, connectionSettings, startPos=None, endPos=None, startTime=None,
                 stopTime=None, only_schemas=None, only_tables=None, nopk=False, flashback=False, stopnever=False):
        '''
        connectionSettings: {'host': 127.0.0.1, 'port': 3306, 'user': slave, 'passwd': slave}
        '''
        #if not startFile:
        #    raise ValueError('lack of parameter,startFile.')

        self.filePath = filePath
        self.connectionSettings = connectionSettings
        self.startPos = startPos if startPos else 4 # use binlog v4
        self.endPos = endPos
        self.startTime = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") if startTime else datetime.datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        self.stopTime = datetime.datetime.strptime(stopTime, "%Y-%m-%d %H:%M:%S") if stopTime else datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.nopk, self.flashback, self.stopnever = (nopk, flashback, stopnever)

        self.binlogList = []
        self.connection = pymysql.connect(**self.connectionSettings)


    def process_binlog(self):
        stream = BinLogFileReader(self.filePath, ctl_connection_settings=self.connectionSettings,
                                    log_pos=self.startPos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True)

        cur = self.connection.cursor()
        tmpFile = create_unique_file('%s.%s' % (self.connectionSettings['host'],self.connectionSettings['port'])) # to simplify code, we do not use file lock for tmpFile.
        ftmp = open(tmpFile ,"w")
        flagLastEvent = False
        eStartPos, lastPos = stream.log_pos, stream.log_pos
        try:
            for binlogevent in stream:
                if not self.stopnever:
                    if datetime.datetime.fromtimestamp(binlogevent.timestamp) < self.startTime:
                        if not (
                            isinstance(binlogevent, RotateEvent) or isinstance(binlogevent, FormatDescriptionEvent)):
                            lastPos = binlogevent.packet.log_pos
                        continue
                    elif datetime.datetime.fromtimestamp(binlogevent.timestamp) >= self.stopTime:
                        break
                    else:
                        pass

                if isinstance(binlogevent, QueryEvent) and binlogevent.query == 'BEGIN':
                    eStartPos = lastPos

                if isinstance(binlogevent, QueryEvent):
                    sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, flashback=self.flashback, nopk=self.nopk)
                    if sql:
                        print sql
                elif isinstance(binlogevent, WriteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, DeleteRowsEvent):
                    for row in binlogevent.rows:
                        sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, row=row , flashback=self.flashback, nopk=self.nopk, eStartPos=eStartPos)
                        if self.flashback:
                            ftmp.write(sql + '\n')
                        else:
                            print sql

                if not (isinstance(binlogevent, RotateEvent) or isinstance(binlogevent, FormatDescriptionEvent)):
                    lastPos = binlogevent.packet.log_pos
                if flagLastEvent:
                    break
            ftmp.close()

            if self.flashback:
                self.print_rollback_sql(tmpFile)
        finally:
            os.remove(tmpFile)
        cur.close()
        stream.close()
        return True

    def print_rollback_sql(self, fin):
        '''print rollback sql from tmpfile'''
        with open(fin) as ftmp:
            sleepInterval = 1000
            i = 0
            for line in reversed_lines(ftmp):
                print line.rstrip()
                if i >= sleepInterval:
                    print 'SELECT SLEEP(1);'
                    i = 0
                else:
                    i += 1

    def __del__(self):
        pass


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    connectionSettings = {'host':args.host, 'port':args.port, 'user':args.user, 'passwd':args.password}
    bin2sql = Bin2sql(filePath=args.file_path[0], connectionSettings=connectionSettings,
                            startPos=args.startPos, endPos=args.endPos,
                            startTime=args.startTime, stopTime=args.stopTime, only_schemas=args.databases,
                            only_tables=args.tables, nopk=args.nopk, flashback=args.flashback, stopnever=args.stopnever)
    bin2sql.process_binlog()