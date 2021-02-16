import sys
import os

sys.path.append(os.path.abspath('../'))
from template.config import RECORD_COLUMN_OFFSET, PAGE_SIZE
from template.table import Record, PhysicalPages
from template.page import Page
from unittest import mock

import unittest


class PhysicalPagesTest(unittest.TestCase):

    def setUp(self):
        self.numColumns = 4 + RECORD_COLUMN_OFFSET
        self.fakeData = []
        for i in range(4):
            self.fakeData.append(i)
        self.physicalPages = PhysicalPages(self.numColumns)

    def test_init(self):
        self.assertEqual(self.physicalPages.numOfRecords, 0)
        self.assertEqual(len(self.physicalPages.physicalPages), self.numColumns)
        for item in self.physicalPages.physicalPages:
            self.assertIsInstance(item, Page)

    def test_hasCapacity(self):
        self.assertTrue(self.physicalPages.hasCapacity())
        self.physicalPages.numOfRecords = PAGE_SIZE
        self.assertFalse(self.physicalPages.hasCapacity())
        self.physicalPages.numOfRecords = PAGE_SIZE-1
        self.assertTrue(self.physicalPages.hasCapacity())
        self.physicalPages.numOfRecords = PAGE_SIZE+1
        self.assertFalse(self.physicalPages.hasCapacity())

    @mock.patch('template.table.Record.getNewRID', return_value=0)
    def test_setPageRecord(self, mockRID):
        record = Record(0,0,0,0,self.fakeData)
        recordLocation = [0,0,0]

        self.assertEqual(self.physicalPages.setPageRecord(record,recordLocation),0)

        self.assertEqual(self.physicalPages.numOfRecords, 1)

        for _ in range(PAGE_SIZE-1):
            self.assertEqual(self.physicalPages.setPageRecord(record,recordLocation),0)

        self.assertEqual(self.physicalPages.numOfRecords, PAGE_SIZE)




if __name__ == '__main__':
    unittest.main()
