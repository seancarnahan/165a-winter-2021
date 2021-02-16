import unittest
from collections import defaultdict
from random import randrange
import sys
import os

sys.path.append(os.path.abspath('../'))
from template.table import Table
from template.config import RECORD_COLUMN_OFFSET

class IndexTester(unittest.TestCase):

    def setUp(self):
        self.tb_name = "TestTable"
        self.numColumns = 5
        self.key = 0
        self.tb = Table(self.tb_name, self.numColumns, self.key)
        self.idx = self.tb.index
        self.rids = []
        self.data = []

    def addData(self):
        for i in range(0, 10000):
            rid = i
            data = (i, i%10, i%100, i%500, 0)
            self.data.append(data)
            self.rids.append(rid)
            self.idx.insert(rid, data)

    def getColumnIndex(self, column):
        return self.idx.indices[column+RECORD_COLUMN_OFFSET]

    def test_init(self):
        self.assertEqual(len(self.idx.indices), self.numColumns + RECORD_COLUMN_OFFSET)
        self.assertEqual(len(self.idx.seeds), self.numColumns + RECORD_COLUMN_OFFSET)
        self.assertEqual(self.idx.table, self.tb)
        self.assertEqual(len(self.idx.indices[RECORD_COLUMN_OFFSET]), 0)
        self.assertEqual(self.idx.seeds[RECORD_COLUMN_OFFSET], None)

    def test_createIndex(self):
        self.assertTrue(self.idx.create_index(1))
        self.assertIsInstance(self.idx.indices[RECORD_COLUMN_OFFSET], defaultdict)
        self.assertEqual(self.idx.seeds[RECORD_COLUMN_OFFSET], None)

    def test_createIndex2(self):
        self.addData()

        colIdx = self.getColumnIndex(0)

        for rid, data in zip(self.rids, self.data):
            keyRecords = colIdx[data[0]][0]
            self.assertEqual(rid, keyRecords[0])

        for data in self.data[:-1]:
            nextKeyInDict = colIdx[data[0]][1]
            self.assertEqual(data[0]+1,nextKeyInDict)

        self.assertEqual(colIdx[self.data[-1][0]][1], None)

    def test_createIndex3(self):
        self.assertTrue(self.idx.create_index(1+RECORD_COLUMN_OFFSET))
        self.addData()

        colIdx = self.getColumnIndex(1)

        for i in range(len(self.data)):
            correctBin = self.data[i][1]
            correctRID = self.rids[i]

            self.assertIn(correctRID, colIdx[correctBin][0])

        for i in range(len(colIdx.items())-1):
            nextKey = i+1
            correctBin = self.data[i][1]
            self.assertEqual(nextKey, colIdx[correctBin][1])

        maxKey = max(colIdx.keys())
        self.assertEqual(None, colIdx[maxKey][1])

    def test_dropIndex(self):
        colIdx = self.getColumnIndex(0)
        self.assertIsInstance(colIdx, defaultdict)
        self.assertTrue(self.idx.drop_index(0+RECORD_COLUMN_OFFSET))
        colIdx = self.getColumnIndex(0)
        self.assertEqual(None, colIdx)

    def test_insertIntoIndex(self):
        colIdx = self.getColumnIndex(0)

        self.idx.insertIntoIndex(0+RECORD_COLUMN_OFFSET, 0, 0)

        self.assertIn(0, colIdx[0][0])
        self.assertEqual(None, colIdx[0][1])

    def test_updateIndex(self):
        colIdx = self.getColumnIndex(0)

        rid = 0
        oldVal = 0
        newVal = 1
        self.idx.insertIntoIndex(0+RECORD_COLUMN_OFFSET, rid, oldVal)
        self.idx.update_index(0+RECORD_COLUMN_OFFSET, rid, oldVal, newVal)

        self.assertNotIn(oldVal, colIdx.keys())
        self.assertIn(rid, colIdx[newVal][0])
        self.assertEqual(None, colIdx[newVal][1])

    def test_updateIndex2(self):
        colIdx = self.getColumnIndex(0)

        rid = (0,1)
        oldVals = (1,2)
        newVals = (2,4)

        self.idx.insertIntoIndex(RECORD_COLUMN_OFFSET, rid[0], oldVals[0])
        self.idx.insertIntoIndex(RECORD_COLUMN_OFFSET, rid[1], oldVals[1])

        self.idx.update_index(RECORD_COLUMN_OFFSET, rid[0], oldVals[0], newVals[0])
        self.assertNotIn(oldVals[0], colIdx.keys())
        self.assertIn(rid[0], colIdx[newVals[0]][0])
        self.assertEqual(None, colIdx[oldVals[1]][1])

        self.idx.update_index(RECORD_COLUMN_OFFSET, rid[1], oldVals[1], newVals[1])
        self.assertNotIn(rid[1], colIdx[oldVals[1]][0])
        self.assertIn(rid[1], colIdx[newVals[1]][0])
        self.assertEqual(newVals[1], colIdx[newVals[0]][1])
        self.assertEqual(None, colIdx[newVals[1]][1])

    def test_removeFromIndex(self):
        colIdx = self.getColumnIndex(0)
        rid = 0
        val = 0

        self.idx.insertIntoIndex(RECORD_COLUMN_OFFSET, rid, val)
        self.idx.insertIntoIndex(RECORD_COLUMN_OFFSET, rid+1, val)
        self.assertIn(rid, colIdx[val][0])
        self.assertIn(rid+1, colIdx[val][0])

        self.idx.removeFromIndex(RECORD_COLUMN_OFFSET, rid+1, val)
        self.assertNotIn(rid+1, colIdx[val][0])
        self.idx.removeFromIndex(RECORD_COLUMN_OFFSET, rid, val)
        self.assertNotIn(val, colIdx.keys())

    def test_createSeeds(self):
        colIdx = self.getColumnIndex(0)

        colIdx[1] = 1
        colIdx[2] = 2
        colIdx[3] = 3

        self.idx.createSeeds(RECORD_COLUMN_OFFSET)

        self.assertListEqual([1,2,3], self.idx.seeds[RECORD_COLUMN_OFFSET])

        colIdx[4] = 4

        self.idx.createSeeds(RECORD_COLUMN_OFFSET)

        self.assertListEqual([1,3,4], self.idx.seeds[RECORD_COLUMN_OFFSET])

    def test_updateMaxSeed(self):
        colIdx = self.getColumnIndex(0)

        colIdx[1] = 1
        colIdx[2] = 2
        colIdx[3] = 3

        self.idx.createSeeds(RECORD_COLUMN_OFFSET)
        self.idx.updateMaxSeed(RECORD_COLUMN_OFFSET, 10)

        self.assertListEqual([1,2,10], self.idx.seeds[RECORD_COLUMN_OFFSET])

    def test_updateMinSeed(self):

        colIdx = self.getColumnIndex(0)

        colIdx[1] = 1
        colIdx[2] = 2
        colIdx[3] = 3

        self.idx.createSeeds(RECORD_COLUMN_OFFSET)

        self.idx.updateMinSeed(RECORD_COLUMN_OFFSET, 0)

        self.assertListEqual([0, 2, 3], self.idx.seeds[RECORD_COLUMN_OFFSET])

    def test_updateMedianSeed(self):

        colIdx = self.getColumnIndex(0)

        colIdx[1] = 1
        colIdx[2] = 2
        colIdx[3] = 3

        self.idx.createSeeds(RECORD_COLUMN_OFFSET)

        colIdx[4] = 4
        self.idx.updateMedianSeed(RECORD_COLUMN_OFFSET, list(colIdx.keys()))

        # I know this doesn't make sense w.r.t the actual keys, but
        # just testing if function grabs middle value of the list
        self.assertListEqual([1, 3, 3], self.idx.seeds[RECORD_COLUMN_OFFSET])


    def test_locate(self):

        self.addData()
        key = 0

        for rid, data in zip(self.rids, self.data):
            value = data[0]
            retrieved_rids = self.idx.locate(key, value)
            self.assertIn(rid, retrieved_rids)

        self.assertListEqual([], self.idx.locate(key, -1))

    def test_locate_range(self):
        self.addData()

        begin = 0
        end = 200
        key = 0

        ridsInRange = self.rids[0:201]
        retrieved_rids = self.idx.locate_range(begin, end, key)

        for rid in retrieved_rids:
            self.assertIn(rid, ridsInRange)
            ridsInRange.remove(rid)

        self.assertListEqual([], ridsInRange)


        begin = -100
        end = -1

        ridsInRange = []
        retrieved_rids = self.idx.locate_range(begin, end, key)

        self.assertListEqual(ridsInRange, retrieved_rids)

        begin = len(self.data) + 100
        end = begin + 200

        ridsInRange = []
        retrieved_rids = self.idx.locate_range(begin, end, key)

        self.assertListEqual(ridsInRange, retrieved_rids)

    def test_rhashLinkedList(self):

        dataset = []
        for i in range(0, 100):
            rid = randrange(1000)
            k = randrange(100)
            data = (k, k%10, k%100, k%500, 0)
            dataset.append((rid, data))
            self.idx.insert(rid, data)

        colIdx = self.getColumnIndex(0)

        sortedKeys = list(colIdx.keys())
        sortedKeys.sort()

        testKey = self.idx.seeds[self.key+RECORD_COLUMN_OFFSET][0]

        for key in sortedKeys:
            self.assertEqual(key, testKey)
            testKey = colIdx[testKey][1]



if __name__ == '__main__':
    unittest.main()
