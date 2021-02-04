import unittest

from template.table import Table
from template.config import RECORD_COLUMN_OFFSET
from template.query import Query


class IndexTester(unittest.TestCase):

    def setUp(self):
        self.tb_name = "TestTable"
        self.numColumns = 4
        self.key = 0
        self.tb = Table(self.tb_name, self.numColumns, self.key)
        self.idx = self.tb.index

    def addData(self):
        query = Query(self.tb)
        for i in range(0, 10000):
            query.insert(906659671 + i, 93, 0, 0, 0)

    def test_init(self):
        self.assertEqual(len(self.idx.indices), self.numColumns + RECORD_COLUMN_OFFSET)
        self.assertEqual(len(self.idx.seeds), self.numColumns + RECORD_COLUMN_OFFSET)
        self.assertEqual(self.idx.table, self.tb)
        self.assertEqual(len(self.idx.indices[RECORD_COLUMN_OFFSET]), 0)
        self.assertEqual(self.idx.seeds[RECORD_COLUMN_OFFSET], None)

    def test_createIndex(self):
        self.assertTrue(self.idx.create_index(1))
        self.assertEqual(self.idx.indices[RECORD_COLUMN_OFFSET + 1], None)
        self.assertEqual(self.idx.seeds[RECORD_COLUMN_OFFSET + 1], None)

    def test_createIndex2(self):
        self.addData()



if __name__ == '__main__':
    unittest.main()
