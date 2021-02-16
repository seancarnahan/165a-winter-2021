import unittest
import sys
import os

sys.path.append(os.path.abspath('../'))
from template.table import Table, Record
from template.query import Query
from template.index import Index
from template.config import *


class TestQueryFunctionality(unittest.TestCase):
    def setUp(self):
        self.name = "grades"
        self.key = 0
        self.num_columns = 5

        self.table = Table(self.name, self.num_columns, self.key)

        self.table.createNewRecord(0, [10, 6, 7, 8, 9])
        self.table.createNewRecord(0, [20, 8, 7, 6, 5])
        self.table.createNewRecord(0, [30, 6, 5, 4, 3])
        self.table.index.create_index(0 + RECORD_COLUMN_OFFSET)
        self.query = Query(self.table)
        self.index = self.table.index

    def test_insert(self):
        insert1 = self.query.insert([1, 2, 3, 4, 5])
        self.assertEqual(True, insert1)

    def test_select(self):
        select1 = self.query.select(20, 4, 0o101)
        select2 = self.query.select(30, 4, 11000)

        self.assertEqual([7, 5], select1)
        self.assertEqual([7, 6], select2)

    def test_update(self):
        update = self.query.update(30, [30, 9, 9, 9, 9])
        self.assertEqual(True, update)

        rid_list = self.index.locate(0, 30)
        rid = rid_list.pop()  # get the only rid from the list of rids returned
        record = self.table.getLatestupdatedRecord(rid)
        self.assertEqual(record.columns, [30, 9, 9, 9, 9])

    def test_delete(self):
        grades = self.table

        corr_table = Table("grades_copy", 5, 0)
        corr_table.createNewRecord(0, [10, 6, 7, 8, 9])
        corr_table.createNewRecord(0, [20, 8, 7, 6, 5])

        self.query.delete(30)

        self.assertNotEqual(grades, corr_table)

    def test_sum(self):
        check_sum = self.query.sum(0, 30, 4)
        # column 0 add first two values

        # key 10, col 0 = 5 within range of 0-8
        # key 30, col 0 = 7 within range of 0-8
        self.assertEqual(17, check_sum)
