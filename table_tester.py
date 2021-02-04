import unittest
import math
from unittest import mock

from template.table import *
from template.index import Index
from template.config import *


class TestTableFunctionality(unittest.TestCase):
    def setUp(self):
        self.name = "grades"
        self.key = 1234
        self.num_columns = 5
  
        self.table = Table(self.name, self.num_columns, self.key)

        self.assertEqual(self.table.name, "grades")
        self.assertEqual(self.table.key, 1234)
        self.assertEqual(self.table.num_columns, 10)
        self.assertEqual(self.table.page_directory.num_columns, 10)
        self.assertEqual(self.table.page_directory.currPageRangeIndex, 0)
        self.assertEqual(self.table.index.table, self.table)
        self.assertEqual(self.table.latestRID, None)
        self.assertEqual(self.table.currPageRangeIndex, 0)

    def test_table_getRecord(self):
        self.table.createNewRecord(1234,[5,6,7,8,9])

        record = self.table.getRecord(100000000)

        self.assertEqual(record.key, 1234)
        self.assertEqual(record.indirection, 0)
        # self.assertEqual(record.timestamp, 100000000)
        self.assertEqual(record.encoding, 0)
        self.assertEqual(record.columns, [5,6,7,8,9])
        self.assertEqual(record.RID, 100000000)

    def test_table_createNewRecord(self):
        numOfRecordsAdded = 10000
        values = [0,0,0,0,0]
        
        for i in range(0,numOfRecordsAdded):
            values = [0,0,0,0,1 + i]
            self.table.createNewRecord(i+1, values)

        #10000 records created, with 10 total columns
        # page = 1000 records
        # 2000 records per pageRange
        # each page range can hold 2 sets of physical pages -> 
        # the Page Directory should have 5 Page Ranges
        pageRanges = self.table.page_directory.pageRanges
        numOfPageRanges = len(pageRanges)
        pageRangeCapacity = self.table.page_directory.pageRanges[0].getPageRangeCapacity()

        #I think this is right
        expectedNumOfPageRanges = math.floor((numOfRecordsAdded / PAGE_SIZE) / pageRangeCapacity)

        self.assertEqual(numOfPageRanges, expectedNumOfPageRanges)

        #loop through table.pagedirectories base pages
        for i in range(0, numOfPageRanges):
            basePages = pageRanges[i].basePages

            self.assertEqual(len(basePages), pageRangeCapacity)

            #loop through each basePage/ set of Physical pages
            for i in range(0, len(basePages)):
                physicalPages = basePages[i]

                self.assertEqual(len(physicalPages.physicalPages), RECORD_COLUMN_OFFSET + len(values))

        #spot check random ids-------------------------- will only work for the 5 column format
        
        #record: 500 -> 1 00 00 0499
        record = self.table.getRecord(100000499)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 500)
        self.assertEqual(RID, 100000499)

        #record: 1000 -> 1 00 00 0999
        record = self.table.getRecord(100000999)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 1000)
        self.assertEqual(RID, 100000999)

        #record: 1001 -> 1 00 01 0000
        record = self.table.getRecord(100010000)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 1001)
        self.assertEqual(RID, 100010000)

        # #record: 2001 -> 1 01 00 0000
        record = self.table.getRecord(101000000)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 2001)
        self.assertEqual(RID, 101000000)
        
        #record: 2600 -> 1 01 00 0599
        record = self.table.getRecord(101000599)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 2600)
        self.assertEqual(RID, 101000599)

        #record: 7999 -> 1 03 01 0998
        record = self.table.getRecord(103010998)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 7999)
        self.assertEqual(RID, 103010998)

        #record 8000 -> 1 03 01 0999
        record = self.table.getRecord(103010999)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 8000)
        self.assertEqual(RID, 103010999)

        #record 8001 -> 1 04 00 0000
        record = self.table.getRecord(104000000)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 8001)
        self.assertEqual(RID, 104000000)

        #record: 10000 -> 1 04 01 0999
        record = self.table.getRecord(104010999)
        RID = record.RID
        fifthColItem = record.columns[4]
        self.assertEqual(fifthColItem, 10000)
        self.assertEqual(RID, 104010999)


    def test_table_updateRecord(self):
        pass

if __name__ == "__main__":
    unittest.main()