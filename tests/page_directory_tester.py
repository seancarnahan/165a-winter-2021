import unittest
import sys
import os

sys.path.append(os.path.abspath('../'))
from template.table import Table
from template.page_directory import PageDirectory
from template.record import Record
from template.page_range import PageRange
from template.physical_pages import PhysicalPages
from unittest import mock

class PageDirectoryTester(unittest.TestCase):

    def setUp(self):
        self.num_columns = 4
        self.testDirectory = PageDirectory(self.num_columns)

    def test_init(self):

        self.assertEqual(self.testDirectory.num_columns, self.num_columns)
        self.assertEqual(len(self.testDirectory.pageRanges), 1)
        self.assertIsInstance(self.testDirectory.pageRanges[0], PageRange)
        self.assertEqual(self.testDirectory.currPageRangeIndex, 0)

    def test_addNewPageRange(self):

        prev_numPageRanges = len(self.testDirectory.pageRanges)
        prev_currPageRangeIndex = self.testDirectory.currPageRangeIndex

        self.testDirectory.addNewPageRange()

        self.assertEqual(self.testDirectory.currPageRangeIndex, prev_currPageRangeIndex+1)
        self.assertEqual(len(self.testDirectory.pageRanges), prev_numPageRanges+1)
        self.assertIsInstance(self.testDirectory.pageRanges[self.testDirectory.currPageRangeIndex], PageRange)

        prev_numPageRanges = len(self.testDirectory.pageRanges)
        prev_currPageRangeIndex = self.testDirectory.currPageRangeIndex

        for _ in range(50):
            prev_currPageRangeIndex += 1
            prev_numPageRanges += 1
            self.testDirectory.addNewPageRange()

        self.assertEqual(self.testDirectory.currPageRangeIndex, prev_currPageRangeIndex)
        self.assertEqual(len(self.testDirectory.pageRanges), prev_numPageRanges)

    @mock.patch('PageRange.insertBaseRecord', return_value=True)
    def test_insertBaseRecord(self, mockInsertBaseRecord):
        key = 1234
        indirection = 0
        timestamp = 123456789
        encoding=0
        columns = [1,2,3,4]
        testRecord = Record(key,indirection,timestamp,encoding,columns)
        self.assertTrue(self.testDirectory.insertBaseRecord(testRecord))

    @mock.patch('template.PageRange.insertBaseRecord', return_value=False)
    def test_insertBaseRecord2(self, mockInsertBaseRecord):
        key = 1234
        indirection = 0
        timestamp = 123456789
        encoding=0
        columns = [1,2,3,4]
        testRecord = Record(key,indirection,timestamp,encoding,columns)

        prev_currPageRangeIndex = self.testDirectory.currPageRangeIndex
        prev_numPageRanges = len(self.testDirectory.pageRanges)

        self.assertTrue(self.testDirectory.insertBaseRecord(testRecord))

        self.assertEqual(self.testDirectory.currPageRangeIndex, prev_currPageRangeIndex+1)
        self.assertEqual(len(self.testDirectory.pageRanges), prev_numPageRanges+1)

    @mock.patch('template.PageDirectory.getRecordLocation', return_value=[0,0,0,0])
    @mock.patch('template.PageRange.insertTailRecord', return_value=True)
    def test_insertTailRecord(self, mockRecordLocation, mockInsertTailRecord):
        self.assertTrue(self.testDirectory.insertTailRecord(0,0))


    def test_getPhysicalPages(self):
        params1 = (0,0,0,0)
        physical_pages = self.testDirectory.getPhysicalPages(*params1)
        self.assertIsInstance(physical_pages, PhysicalPages)
        params2 = (1,0,0,0)
        physical_pages = self.testDirectory.getPhysicalPages(*params2)
        self.assertIsInstance(physical_pages, PhysicalPages)

    @unittest.skip("getRecordLocation has not been implemented")
    def test_getRecordLocation(self):
        pass


if __name__ == '__main__':
    unittest.main()
