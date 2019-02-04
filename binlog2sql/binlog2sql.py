#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import command_line_args, concat_sql_from_binlog_event, create_unique_file, temp_open, \
    reversed_lines, is_dml_event, event_type


class Binlog2sql(object):

    def __init__(self, connection_settings, start_file, start_pos, end_file, end_pos, start_time, stop_time,
                 only_schemas, only_tables, sql_type, no_pk=False, flashback=False, stop_never=False,
                 only_dml=True, back_interval=0.1):
        """
        connection_settings: {'host': 127.0.0.1, 'port': 3306, 'user': user, 'passwd': passwd, 'charset': 'utf8'}
        """

        if not start_file:
            raise ValueError('Lack of parameter: start_file')

        if start_time:
            self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.datetime.strptime('1980-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")

        if stop_time:
            self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.stop_time = datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")

        self.conn = pymysql.connect(**connection_settings)
        self.conn.autocommit(True)

        self.instance = "{0}:{1}".format(connection_settings['host'], connection_settings['port'])
        self.eof_file, self.eof_pos = self.get_master_status()
        self.end_file = end_file if end_file else start_file
        self.end_pos = end_pos
        self.binlog_list = self.get_binlog_list(start_file, self.end_file)
        server_id = self.get_server_id()
        start_pos = start_pos if start_pos else 4    # use binlog v4
        self.stream = BinLogStreamReader(connection_settings=connection_settings, server_id=server_id,
                                         log_file=start_file, log_pos=start_pos, only_schemas=only_schemas,
                                         only_tables=only_tables, resume_stream=True, blocking=True)

        self.no_pk, self.flashback, self.stop_never = (no_pk, flashback, stop_never)
        self.only_dml, self.back_interval = (only_dml, back_interval)
        self.sql_type = [t.upper() for t in sql_type] if sql_type else ['INSERT', 'UPDATE', 'DELETE']

    def get_binlog_list(self, start_file, end_file):
        binlog_list = []
        with self.conn as cursor:
            cursor.execute("SHOW MASTER LOGS")
            bin_index = [row[0] for row in cursor.fetchall()]
            if start_file not in bin_index:
                raise ValueError('parameter error: start_file {0} not in mysql instance'.format(start_file))
            binlog2i = lambda x: x.split('.')[1]
            for binary in bin_index:
                if binlog2i(start_file) <= binlog2i(binary) <= binlog2i(end_file):
                    binlog_list.append(binary)
        return binlog_list

    def get_master_status(self):
        with self.conn as cursor:
            cursor.execute("SHOW MASTER STATUS")
            return cursor.fetchone()[:2]

    def get_server_id(self):
        with self.conn as cursor:
            cursor.execute("SELECT @@server_id")
            server_id = cursor.fetchone()[0]
            if not server_id:
                raise ValueError('missing server_id in mysql instance')
            return server_id

    def position(self, log_file, log_pos, event_time):
        """position compared to start-stop section"""
        if (log_file not in self.binlog_list) or (event_time >= self.stop_time) or \
                (self.end_pos and log_file == self.end_file and log_pos > self.end_pos) or \
                (log_file == self.eof_file and log_pos > self.eof_pos):
            position = 'after'
        elif (log_file == self.end_file and log_pos == self.end_pos) or \
                (log_file == self.eof_file and log_pos == self.eof_pos):
            position = 'end'
        elif event_time < self.start_time:
            position = 'before'
        elif event_time >= self.start_time:
            position = 'in'
        return position

    def process_binlog(self):
        flag_last_event = False
        e_start_pos, last_pos = self.stream.log_pos, self.stream.log_pos

        # to simplify code, I do not use flock for tmp_file.
        tmp_file = create_unique_file(self.instance)

        with temp_open(tmp_file, "w") as f_tmp, self.conn as cursor:
            for binlog_event in self.stream:
                print(binlog_event)
                log_file = self.stream.log_file
                log_pos = self.stream.log_pos
                try:
                    event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                except OSError:
                    event_time = datetime.datetime(1980, 1, 1, 0, 0)

                position = self.position(log_file, log_pos, event_time)
                if position == 'after' and not self.stop_never:
                    break
                elif position == 'end' and not self.stop_never:
                    flag_last_event = True
                elif position == 'before':
                    if not (isinstance(binlog_event, RotateEvent)
                            or isinstance(binlog_event, FormatDescriptionEvent)):
                        last_pos = binlog_event.packet.log_pos
                    continue

                # 此处有问题，updateevent变成了queryevent
                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event,
                                                       flashback=self.flashback, no_pk=self.no_pk)
                    if sql:
                        print(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        sql = concat_sql_from_binlog_event(cursor=cursor, binlog_event=binlog_event, no_pk=self.no_pk,
                                                           row=row, flashback=self.flashback, e_start_pos=e_start_pos)
                        if self.flashback:
                            f_tmp.write(sql + '\n')
                        else:
                            print(sql)

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break

            if self.flashback:
                self.print_rollback_sql(filename=tmp_file)
        return True

    def print_rollback_sql(self, filename):
        """print rollback sql from tmp_file"""
        with open(filename, "rb") as f_tmp:
            batch_size = 1000
            i = 0
            for line in reversed_lines(f_tmp):
                print(line.rstrip())
                if i >= batch_size:
                    i = 0
                    if self.back_interval:
                        print('SELECT SLEEP({0});'.format(self.back_interval))
                else:
                    i += 1

    def __del__(self):
        self.stream.close()
        self.conn.close()


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    conn_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    binlog2sql = Binlog2sql(connection_settings=conn_setting, start_file=args.start_file, start_pos=args.start_pos,
                            end_file=args.end_file, end_pos=args.end_pos, start_time=args.start_time,
                            stop_time=args.stop_time, only_schemas=args.databases, only_tables=args.tables,
                            no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
                            back_interval=args.back_interval, only_dml=args.only_dml, sql_type=args.sql_type)
    binlog2sql.process_binlog()
