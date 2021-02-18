import unittest
import sys
import os

sys.path.append(os.path.abspath('../'))
from template.table import *
from unittest import mock

class RecordTester(unittest.TestCase):

    def setUp(self):
        self.key = 1234
        self.indirection = 0
        self.timestamp = 12345678
        self.encoding = 0
        self.columns = [1,2,3,4]

        self.record = Record(self.key, self.indirection, self.timestamp, self.encoding, self.columns)

        self.assertEqual(self.record.key, self.key)
        self.assertEqual(self.record.indirection, self.indirection)
        self.assertEqual(self.record.timestamp, self.timestamp)
        self.assertEqual(self.record.encoding, self.encoding)
        self.assertEqual(self.record.columns, self.columns)

    def test_getNewRID(self):
        recordType = 1
        locPRIndex = 23
        locBPIndex = 45
        locPhyPageIndex = 6789

        RID = self.record.getNewRID(recordType, locPRIndex, locBPIndex, locPhyPageIndex)

        self.assertEqual(RID, 123456789)

if __name__ == '__main__':
    unittest.main()
    
