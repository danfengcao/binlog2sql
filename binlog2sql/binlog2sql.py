#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import command_line_args, concat_sql_from_binlogevent, create_unique_file

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
        self.popPk, self.flashback, self.stopnever = (popPk, flashback, stopnever)

        self.binlogList = []
        self.connection = pymysql.connect(**self.connectionSettings)
        try:
            cur = self.connection.cursor()
            cur.execute("SHOW MASTER STATUS")
            self.eofFile, self.eofPos = cur.fetchone()[:2]
            cur.execute("SHOW MASTER LOGS")
            binIndex = [row[0] for row in cur.fetchall()]
            if self.startFile not in binIndex:
                raise ValueError('parameter error: startFile %s not in mysql server' % self.startFile)
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
        tmpFile = create_unique_file('%s.%s' % (self.connectionSettings['host'],self.connectionSettings['port'])) # to simplify code, we do not use file lock for tmpFile.
        ftmp = open(tmpFile ,"w")
        flagLastEvent = False
        eStartPos, lastPos = stream.log_pos, stream.log_pos
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
