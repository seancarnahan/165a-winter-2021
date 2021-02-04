import unittest
from template.table import *
from template.index import Index
from template.query import *
from template.config import *


class TestQueryFunctionality(unittest.TestCase):
    def setUp(self):
        self.name = "grades"
        self.key = 1234
        self.num_columns = 5

        self.table = Table(self.name, self.num_columns, self.key)
        self.table.createNewRecord(10, [5, 6, 7, 8, 9])
        self.table.createNewRecord(20, [9, 8, 7, 6, 5])
        self.table.createNewRecord(30, [7, 6, 5, 4, 3])
        self.table.index.create_index(4)
        self.table.index.create_index(0 + RECORD_COLUMN_OFFSET)

        self.query = Query(self.table)
        self.index = Index(self.table)

    def test_insert(self):
        insert1 = self.query.insert([1, 2, 3, 4, 5])
        self.assertEqual(insert1, True)

    def test_select(self):
        select1 = self.query.select(20, 4, 0o101)
        select2 = self.query.select(30, 4, 11000)

        self.assertEqual(select1, [7, 5])
        self.assertEqual(select2, [7, 6])

    def test_update(self):
        update = self.query.update(30, [9, 9, 9, 9, 9])
        self.assertEqual(update, True)

        rid = self.index.locate(4, 30)
        record = self.table.getRecord(rid)
        self.assertEqual(record.columns, [9, 9, 9, 9, 9])

    def test_delete(self):
        grades = self.table

        corr_table = Table("grades_copy", 5, 2345)
        corr_table.createNewRecord(10, [5, 6, 7, 8, 9])
        corr_table.createNewRecord(20, [9, 8, 7, 6, 5])

        self.query.delete(30)

        self.assertNotEqual(grades, corr_table)

    def test_sum(self):
        check_sum = self.query.sum(0, 8, 0 + RECORD_COLUMN_OFFSET)
        # column 0 add first two values

        # key 10, col 0 = 5 within range of 0-8
        # key 30, col 0 = 7 within range of 0-8
        self.assertEqual(check_sum, 12)
