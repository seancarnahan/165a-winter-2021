import unittest
import sys
import os

sys.path.append(os.path.abspath('../'))
from template.config import PAGE_SIZE, PAGE_RECORD_SIZE
from template.page import Page
from unittest.mock import patch


class PageTester(unittest.TestCase):

    def setUp(self):
        self.testPage = Page()

    def test_init(self):
        self.assertEqual(self.testPage.num_records, 0)
        self.assertIsInstance(self.testPage.data, bytearray)

    def test_hasCapacity(self):
        self.assertTrue(self.testPage.has_capacity())
        self.testPage.num_records = PAGE_SIZE
        self.assertFalse(self.testPage.has_capacity())

    @patch('template.page.Page.has_capacity', return_value=True)
    def test_write(self, mockHasCapacity):
        self.assertTrue(self.testPage.write(12345))
        self.assertEqual(self.testPage.num_records, 1)
        self.assertEqual(len(self.testPage.data), 4)

        for i in range(50):
            self.assertTrue(self.testPage.write(i))

        self.assertEqual(self.testPage.num_records, 51)
        self.assertEqual(len(self.testPage.data), 4 * 51)

    @patch('template.page.Page.has_capacity', return_value=False)
    def test_write2(self, mockHasCapacity):
        self.assertFalse(self.testPage.write(12345))
        self.assertEqual(self.testPage.num_records, 0)
        self.assertEqual(len(self.testPage.data), 0)

    def test_getRecord(self):
        self.testPage.data += b'\x00\x00\x00\x05'
        self.assertEqual(self.testPage.getRecord(0), 5)

        self.assertFalse(self.testPage.getRecord(5))

    def test_replaceRecord(self):
        self.testPage.data += b'\x00\x00\x00\x05'
        self.assertTrue(self.testPage.replaceRecord(0, 10))
        self.assertEqual(self.testPage.data[0:4], b'\x00\x00\x00\x0a')
        self.assertFalse(self.testPage.replaceRecord(4, 10))

    def test_getRecordIndexes(self):
        for i in range(100):
            expectedIndex = [PAGE_RECORD_SIZE * i, PAGE_RECORD_SIZE * (i + 1)]
            self.assertEqual(Page.getRecordIndexes(i), expectedIndex)

    def test_integerToBytes(self):
        integers = [10, 11, 13, 15, 16, 17]
        expectedBytes = [b'\x00\x00\x00\x0a', b'\x00\x00\x00\x0b', b'\x00\x00\x00\x0d', b'\x00\x00\x00\x0f',
                         b'\x00\x00\x00\x10', b'\x00\x00\x00\x11']

        for integer, expectedByte in zip(integers, expectedBytes):
            self.assertEqual(Page.integerToBytes(integer), expectedByte)

    def test_bytesToHex(self):
        bytes = [b'\x00\x00\x00\x0a', b'\x00\x00\x00\x0b', b'\x00\x00\x00\x0d', b'\x00\x00\x00\x0f',
                 b'\x00\x00\x00\x10', b'\x00\x00\x00\x11']
        expectedHexs = ['0000000a', '0000000b', '0000000d', '0000000f', '00000010', '00000011']

        for testByte, expectedHex in zip(bytes, expectedHexs):
            self.assertEqual(Page.bytesToHex(testByte), expectedHex)

    def test_hexToInt(self):
        hexVals = ['0000000a', '0000000b', '0000000d', '0000000f', '00000010', '00000011']
        expectedVals = [10, 11, 13, 15, 16, 17]

        for hexVal, expectedVal in zip(hexVals, expectedVals):
            self.assertEqual(Page.hexToInt(hexVal), expectedVal)


if __name__ == "__main__":
    unittest.main()
