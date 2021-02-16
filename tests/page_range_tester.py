import unittest
import math
import sys
import os

sys.path.append(os.path.abspath('../'))
from template.config import MAX_PAGE_RANGE_SIZE, PAGE_SIZE
from template.table import PageRange, PhysicalPages, Record
from template.config import *
from unittest import mock


class PageRangeTester(unittest.TestCase):

    def setUp(self):
        self.numColumns = 4 + RECORD_COLUMN_OFFSET
        self.fakeDataColumns = [1, 2, 3, 4]
        self.pageRange = PageRange(self.numColumns)

    def test_init(self):
        self.assertEqual(self.numColumns, self.pageRange.num_columns)
        self.assertEqual(math.floor(MAX_PAGE_RANGE_SIZE / self.numColumns), self.pageRange.getPageRangeCapacity())
        self.assertEqual(self.pageRange.currBasePageIndex, 0)
        self.assertEqual(self.pageRange.currTailPageIndex, 0)
        self.assertIsInstance(self.pageRange.basePages[0], PhysicalPages)
        self.assertIsInstance(self.pageRange.tailPages[0], PhysicalPages)

    def test_insertBaseRecord(self):
        record = Record(0, 0, 12345, 0, self.fakeDataColumns)
        recordLocation = [0, 0]

        self.assertTrue(self.pageRange.insertBaseRecord(record, recordLocation))

        for _ in range(PAGE_SIZE - 1):
            recordLocation = [0, 0]
            self.assertTrue(self.pageRange.insertBaseRecord(record, recordLocation))

        # pageRange has filled the first base page

        self.assertEqual(len(self.pageRange.basePages), 1)
        self.assertEqual(self.pageRange.currBasePageIndex, 0)

        self.assertTrue(self.pageRange.insertBaseRecord(record, recordLocation))

        self.assertEqual(len(self.pageRange.basePages), 2)
        self.assertEqual(self.pageRange.currBasePageIndex, 1)

    @mock.patch('template.table.PageRange.hasCapacity', return_value=False)
    @mock.patch('template.table.PhysicalPages.hasCapacity', return_value=False)
    def test_insertBaseRecord2(self, mockPageRangeHasCapacity, mockPhysicalPagesHasCapacity):
        record = Record(0, 0, 12345, 0, self.fakeDataColumns)
        recordLocation = [0, 0]

        self.assertFalse(self.pageRange.insertBaseRecord(record, recordLocation))

    @mock.patch('template.table.PhysicalPages.hasCapacity', return_value=True)
    def test_insertTailRecord(self, mockHasCapacity):
        record = Record(0, 0, 12345, 0, self.fakeDataColumns)
        recordLocation = [2, 0]

        self.assertTrue(self.pageRange.insertTailRecord(record, recordLocation))

    @mock.patch('template.table.PhysicalPages.hasCapacity', return_value=False)
    def test_insertTailRecord2(self, mockHasCapacity):
        record = Record(0, 0, 12345, 0, self.fakeDataColumns)
        recordLocation = [2, 0]

        self.assertEqual(len(self.pageRange.tailPages),1)
        self.assertEqual(self.pageRange.currTailPageIndex, 0)
        self.assertTrue(self.pageRange.insertTailRecord(record,recordLocation))
        self.assertEqual(len(self.pageRange.tailPages),2)
        self.assertEqual(self.pageRange.currTailPageIndex, 1)

    def test_addNewBasePage(self):
        self.assertEqual(self.pageRange.currBasePageIndex, 0)
        self.assertEqual(len(self.pageRange.basePages), 1)
        self.assertTrue(self.pageRange.addNewBasePage())
        self.assertEqual(self.pageRange.currBasePageIndex, 1)
        self.assertEqual(len(self.pageRange.basePages), 2)
        self.assertIsInstance(self.pageRange.basePages[-1], PhysicalPages)

    @mock.patch('template.table.PageRange.hasCapacity', return_value=False)
    def test_addNewBasePage2(self, mockHasCapacity):
        self.assertFalse(self.pageRange.addNewBasePage())

    def test_addNewTailPage(self):
        self.assertEqual(self.pageRange.currTailPageIndex, 0)
        self.assertEqual(len(self.pageRange.tailPages), 1)
        self.pageRange.addNewTailPage()
        self.assertEqual(self.pageRange.currTailPageIndex, 1)
        self.assertEqual(len(self.pageRange.tailPages), 2)
        self.assertIsInstance(self.pageRange.tailPages[-1], PhysicalPages)

    @unittest.skip("Confusion on getPageRangeCapacity")
    def test_getPageRangeCapacity(self):
        pass

    def test_hasCapacity(self):
        self.assertTrue(self.pageRange.hasCapacity())

        for _ in range(int(self.pageRange.maxNumOfBasePages) - 1):
            self.pageRange.basePages.append(PhysicalPages)

        self.assertFalse(self.pageRange.hasCapacity())


if __name__ == '__main__':
    unittest.main()
