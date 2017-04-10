# -*- coding: utf-8 -*-

import pymysql
import struct

from pymysql.constants.COMMAND import COM_BINLOG_DUMP, COM_REGISTER_SLAVE
from pymysql.cursors import DictCursor
from pymysql.util import int2byte

from pymysqlreplication.packet import BinLogPacketWrapper
from pymysqlreplication.constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT
from pymysqlreplication.gtid import GtidSet
from pymysqlreplication.event import (
    QueryEvent, RotateEvent, FormatDescriptionEvent,
    XidEvent, GtidEvent, StopEvent,
    BeginLoadQueryEvent, ExecuteLoadQueryEvent,
    HeartbeatLogEvent, NotImplementedEvent)
from pymysqlreplication.row_event import (
    UpdateRowsEvent, WriteRowsEvent, DeleteRowsEvent, TableMapEvent)

try:
    from pymysql.constants.COMMAND import COM_BINLOG_DUMP_GTID
except ImportError:
    # Handle old pymysql versions
    # See: https://github.com/PyMySQL/PyMySQL/pull/261
    COM_BINLOG_DUMP_GTID = 0x1e

from StringIO import StringIO
from pymysql.util import byte2int

# 2013 Connection Lost
# 2006 MySQL server has gone away
MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]


class StringIOAdvance(StringIO):
    def advance(self, length):
        self.seek(self.tell() + length)


class BinLogFileReader(object):

    """Connect to replication stream and read event
    """
    report_slave = None
    _expected_magic = b'\xfebin'

    def __init__(self, file_path, ctl_connection_settings=None, resume_stream=False,
                 blocking=False, only_events=None, log_file=None, log_pos=None,
                 filter_non_implemented_events=True,
                 ignored_events=None, auto_position=None,
                 only_tables=None, ignored_tables=None,
                 only_schemas=None, ignored_schemas=None,
                 freeze_schema=False, skip_to_timestamp=None,
                 report_slave=None, slave_uuid=None,
                 pymysql_wrapper=None,
                 fail_on_table_metadata_unavailable=False,
                 slave_heartbeat=None):

        # open file
        self._file = None
        self._file_path = file_path
        self._pos = None

        self.__connected_ctl = False
        self._ctl_connection = None
        self._ctl_connection_settings = ctl_connection_settings
        if ctl_connection_settings:
            self._ctl_connection_settings.setdefault("charset", "utf8")

        self.__only_tables = only_tables
        self.__ignored_tables = ignored_tables
        self.__only_schemas = only_schemas
        self.__ignored_schemas = ignored_schemas
        self.__freeze_schema = freeze_schema
        self.__allowed_events = self._allowed_event_list(
            only_events, ignored_events, filter_non_implemented_events)
        self.__fail_on_table_metadata_unavailable = fail_on_table_metadata_unavailable

        # We can't filter on packet level TABLE_MAP and rotate event because
        # we need them for handling other operations
        self.__allowed_events_in_packet = frozenset(
            [TableMapEvent, RotateEvent]).union(self.__allowed_events)

        self.__use_checksum = self.__checksum_enabled()

        # Store table meta information
        self.table_map = {}
        self.log_pos = log_pos
        self.log_file = log_file
        self.auto_position = auto_position
        self.skip_to_timestamp = skip_to_timestamp

        self.report_slave = None
        self.slave_uuid = slave_uuid
        self.slave_heartbeat = slave_heartbeat

        if pymysql_wrapper:
            self.pymysql_wrapper = pymysql_wrapper
        else:
            self.pymysql_wrapper = pymysql.connect

    def close(self):
        if self._file:
            self._file.close()
            self._file_path = None
        if self.__connected_ctl:
            self._ctl_connection._get_table_information = None
            self._ctl_connection.close()
            self.__connected_ctl = False

    def __connect_to_ctl(self):
        self._ctl_connection_settings["db"] = "information_schema"
        self._ctl_connection_settings["cursorclass"] = DictCursor
        self._ctl_connection = self.pymysql_wrapper(**self._ctl_connection_settings)
        self._ctl_connection._get_table_information = self.__get_table_information
        self.__connected_ctl = True

    def __checksum_enabled(self):
        """Return True if binlog-checksum = CRC32. Only for MySQL > 5.6"""
        try:
            self._ctl_connection.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
            result = self._ctl_connection.fetchone()

            if result is None:
                return False
            var, value = result[:2]
            if value == 'NONE':
                return False
            return True
        except Exception:
            return True

    def __connect_to_stream(self):
        if self._file is None:
            self._file = open(self._file_path, 'rb+')
            self._pos = self._file.tell()
            assert self._pos == 0
        # read magic
        if self._pos == 0:
            magic = self._file.read(4)
            if magic == self._expected_magic:
                self._pos += len(magic)
            else:
                messagefmt = 'Magic bytes {0!r} did not match expected {1!r}'
                message = messagefmt.format(magic, self._expected_magic)
                raise BadMagicBytesError(message)

    def fetchone(self):
        while True:
            if not self._file:
                self.__connect_to_stream()

            if not self.__connected_ctl and self._ctl_connection_settings:
                self.__connect_to_ctl()

            # read pkt
            pkt = StringIOAdvance()
            # headerlength 19
            header = self._file.read(19)
            if not header:
                break

            unpacked = struct.unpack('<IcIIIH', header)
            timestamp = unpacked[0]
            event_type = byte2int(unpacked[1])
            server_id = unpacked[2]
            event_size = unpacked[3]
            log_pos = unpacked[4]
            flags = unpacked[5]

            body = self._file.read(event_size - 19)
            pkt.write('0')
            pkt.write(header)
            pkt.write(body)
            pkt.seek(0)

            binlog_event = BinLogPacketWrapper(pkt, self.table_map,
                                               self._ctl_connection,
                                               self.__use_checksum,
                                               self.__allowed_events_in_packet,
                                               self.__only_tables,
                                               self.__ignored_tables,
                                               self.__only_schemas,
                                               self.__ignored_schemas,
                                               self.__freeze_schema,
                                               self.__fail_on_table_metadata_unavailable)

            if not binlog_event.event:
                continue

            if binlog_event.event_type == ROTATE_EVENT:
                self.log_pos = binlog_event.event.position
                self.log_file = binlog_event.event.next_binlog
                # Table Id in binlog are NOT persistent in MySQL - they are in-memory identifiers
                # that means that when MySQL master restarts, it will reuse same table id for different tables
                # which will cause errors for us since our in-memory map will try to decode row data with
                # wrong table schema.
                # The fix is to rely on the fact that MySQL will also rotate to a new binlog file every time it
                # restarts. That means every rotation we see *could* be a sign of restart and so potentially
                # invalidates all our cached table id to schema mappings. This means we have to load them all
                # again for each logfile which is potentially wasted effort but we can't really do much better
                # without being broken in restart case
                self.table_map = {}
            elif binlog_event.log_pos:
                self.log_pos = binlog_event.log_pos

            # This check must not occur before clearing the ``table_map`` as a
            # result of a RotateEvent.
            #
            # The first RotateEvent in a binlog file has a timestamp of
            # zero.  If the server has moved to a new log and not written a
            # timestamped RotateEvent at the end of the previous log, the
            # RotateEvent at the beginning of the new log will be ignored
            # if the caller provided a positive ``skip_to_timestamp``
            # value.  This will result in the ``table_map`` becoming
            # corrupt.
            #
            # https://dev.mysql.com/doc/internals/en/event-data-for-specific-event-types.html
            # From the MySQL Internals Manual:
            #
            #   ROTATE_EVENT is generated locally and written to the binary
            #   log on the master. It is written to the relay log on the
            #   slave when FLUSH LOGS occurs, and when receiving a
            #   ROTATE_EVENT from the master. In the latter case, there
            #   will be two rotate events in total originating on different
            #   servers.
            #
            #   There are conditions under which the terminating
            #   log-rotation event does not occur. For example, the server
            #   might crash.
            if self.skip_to_timestamp and binlog_event.timestamp < self.skip_to_timestamp:
                continue

            if binlog_event.event_type == TABLE_MAP_EVENT and \
                    binlog_event.event is not None:
                self.table_map[binlog_event.event.table_id] = \
                    binlog_event.event.get_table()

            # event is none if we have filter it on packet level
            # we filter also not allowed events
            if binlog_event.event is None or (binlog_event.event.__class__ not in self.__allowed_events):
                continue

            return binlog_event.event

    def _allowed_event_list(self, only_events, ignored_events,
                            filter_non_implemented_events):
        if only_events is not None:
            events = set(only_events)
        else:
            events = set((
                QueryEvent,
                RotateEvent,
                StopEvent,
                FormatDescriptionEvent,
                XidEvent,
                GtidEvent,
                BeginLoadQueryEvent,
                ExecuteLoadQueryEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                DeleteRowsEvent,
                TableMapEvent,
                HeartbeatLogEvent,
                NotImplementedEvent,
                ))
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)
        if filter_non_implemented_events:
            try:
                events.remove(NotImplementedEvent)
            except KeyError:
                pass
        return frozenset(events)

    def __get_table_information(self, schema, table):
        for i in range(1, 3):
            try:
                if not self.__connected_ctl:
                    self.__connect_to_ctl()

                cur = self._ctl_connection.cursor()
                cur.execute("""
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = %s AND table_name = %s
                    """, (schema, table))

                return cur.fetchall()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code in MYSQL_EXPECTED_ERROR_CODES:
                    self.__connected_ctl = False
                    continue
                else:
                    raise error

    def __iter__(self):
        return iter(self.fetchone, None)

class BadMagicBytesError(Exception):
    '''The binlog file magic bytes did not match the specification'''
    pass

class EventSizeTooSmallError(Exception):
    '''The event size was smaller than the length of the event header'''
    pass