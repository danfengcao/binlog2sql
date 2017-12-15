#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import unittest
import mock

sys.path.append("..")
from binlog2sql.binlog2sql_util import *


class TestBinlog2sqlUtil(unittest.TestCase):

    def test_is_valid_datetime(self):
        self.assertTrue(is_valid_datetime('2015-12-12 12:12:12'))
        self.assertFalse(is_valid_datetime('2015-12-12 12:12'))
        self.assertFalse(is_valid_datetime('2015-12-12'))
        self.assertFalse(is_valid_datetime(None))

    @mock.patch('binlog2sql.binlog2sql_util.os.path')
    def test_create_unique_file(self, mock_path):
        filename = "test.sql"
        mock_path.exists.return_value = False
        self.assertEqual(create_unique_file(filename), filename)
        mock_path.exists.return_value = True
        try:
            create_unique_file(filename)
        except Exception as e:
            self.assertEqual(str(e), "cannot create unique file %s.[0-1000]" % filename)

    def test_command_line_args(self):
        try:
            command_line_args(['--flashback', '--no-primary-key'])
        except Exception as e:
            self.assertEqual(str(e), "Lack of parameter: start_file")
        try:
            command_line_args(['--start-file', 'mysql-bin.000058', '--flashback', '--no-primary-key'])
        except Exception as e:
            self.assertEqual(str(e), "Only one of flashback or no_pk can be True")
        try:
            command_line_args(['--start-file', 'mysql-bin.000058', '--flashback', '--stop-never'])
        except Exception as e:
            self.assertEqual(str(e), "Only one of flashback or stop-never can be True")
        try:
            command_line_args(['--start-file', 'mysql-bin.000058', '--start-datetime', '2016-12-12'])
        except Exception as e:
            self.assertEqual(str(e), "Incorrect datetime argument")

    def test_compare_items(self):
        self.assertEqual(compare_items(('data', '12345')), '`data`=%s')
        self.assertEqual(compare_items(('data', None)), '`data` IS %s')

    def test_fix_object(self):
        self.assertEqual(fix_object('ascii'), 'ascii')
        self.assertEqual(fix_object(u'unicode'), u'unicode'.encode('utf-8'))

    def test_generate_sql_pattern(self):
        row = {'values': {'data': 'hello', 'id': 1}}
        mock_write_event = mock.create_autospec(WriteRowsEvent)
        mock_write_event.schema = 'test'
        mock_write_event.table = 'tbl'
        mock_write_event.primary_key = 'id'
        pattern = generate_sql_pattern(binlog_event=mock_write_event, row=row, flashback=False, no_pk=False)
        self.assertEqual(pattern, {'values': ['hello', 1],
                                   'template': 'INSERT INTO `test`.`tbl`(`data`, `id`) VALUES (%s, %s);'})
        pattern = generate_sql_pattern(binlog_event=mock_write_event, row=row, flashback=True, no_pk=False)
        self.assertEqual(pattern, {'values': ['hello', 1],
                                   'template': 'DELETE FROM `test`.`tbl` WHERE `data`=%s AND `id`=%s LIMIT 1;'})
        pattern = generate_sql_pattern(binlog_event=mock_write_event, row=row, flashback=False, no_pk=True)
        self.assertEqual(pattern, {'values': ['hello'], 'template': 'INSERT INTO `test`.`tbl`(`data`) VALUES (%s);'})

        row = {'values':{'data':'hello','id':1}}
        mock_delete_event = mock.create_autospec(DeleteRowsEvent)
        mock_delete_event.schema = 'test'
        mock_delete_event.table = 'tbl'
        pattern = generate_sql_pattern(binlog_event=mock_delete_event, row=row, flashback=False, no_pk=False)
        self.assertEqual(pattern, {'values': ['hello', 1],
                                   'template': 'DELETE FROM `test`.`tbl` WHERE `data`=%s AND `id`=%s LIMIT 1;'})
        pattern = generate_sql_pattern(binlog_event=mock_delete_event, row=row, flashback=True, no_pk=False)
        self.assertEqual(pattern, {'values': ['hello', 1],
                                   'template': 'INSERT INTO `test`.`tbl`(`data`, `id`) VALUES (%s, %s);'})

        row = {'before_values': {'data': 'hello', 'id': 1}, 'after_values': {'data': 'binlog2sql', 'id': 1}}
        mock_update_event = mock.create_autospec(UpdateRowsEvent)
        mock_update_event.schema = 'test'
        mock_update_event.table = 'tbl'
        pattern = generate_sql_pattern(binlog_event=mock_update_event, row=row, flashback=False, no_pk=False)
        self.assertEqual(pattern, {'values': ['binlog2sql', 1, 'hello', 1],
                                   'template': 'UPDATE `test`.`tbl` SET `data`=%s, `id`=%s WHERE `data`=%s AND'
                                               ' `id`=%s LIMIT 1;'})
        pattern = generate_sql_pattern(binlog_event=mock_update_event, row=row, flashback=True, no_pk=False)
        self.assertEqual(pattern, {'values': ['hello', 1, 'binlog2sql', 1],
                                   'template': 'UPDATE `test`.`tbl` SET `data`=%s, `id`=%s WHERE `data`=%s AND'
                                               ' `id`=%s LIMIT 1;'})


if __name__ == '__main__':
    unittest.main()
