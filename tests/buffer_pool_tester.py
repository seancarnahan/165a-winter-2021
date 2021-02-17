import unittest
import sys
import os

sys.path.append(os.path.abspath('../'))
from template.buffer_pool import BufferPool
from unittest import mock

class BufferPoolTester(unittest.TestCase):
    def setUp(self):
        pass

    def test_create_file(self):
        bufferPool = BufferPool()

        directoryLocation = "../disk/"

        fileName = bufferPool.create_file("0", "2", "1", "1")
        
        #test file gets created
        expected = "<_io.TextIOWrapper name='../disk/0211.txt' mode='r' encoding='UTF-8'>"
        notExpected = "<_io.TextIOWrapper name='../disk/0212.txt' mode='r' encoding='UTF-8'>"
        
        new_file = open(fileName, "r")

        self.assertEqual(str(new_file), expected)
        self.assertNotEqual(str(new_file), notExpected)
        
        new_file.close()

        #remove file
        os.remove(fileName)

    def test_write_to_disk(self):
        pass



if __name__ == "__main__":
    unittest.main()
