from template.table import Table
import unittest

class TestTableFunctionality(unittest.TestCase):

    ### ---TEST RECORD CLASS--------------------------------------
    def test_record_init(self):
        pass

    def test_record_getNewRID(self):
        pass


    ### ---TEST TABLE CLASS-----------------------------------
    def test_table_init(self):
        pass

    def test_table_create_table(self):
        pass
        # self.assertEqual(9, 10)

    def test_table_updateRecord(self):
        pass


    ### ---TEST PHYSICALPAGES CLASS--------------------------------------
    def test_physicalPages_init(self):
        pass

    def test_physicalPages_setPageRecord(self):
        pass

    def test_physicalPages_hasCapacity(self):
        pass


    ### ---TEST PAGE RANGE--------------------------------------
    def test_pageRange_init(self):
        pass

    def test_pageRange_insertBaseRecord(self):
        pass

    def test_pageRange_insertTailRecord(self):
        pass

    def test_pageRange_addNewBasePage(self):
        pass

    def test_pageRange_addNewTailPage(self):
        pass

    def test_pageRange_getPageRangeCapacity(self):
        pass

    def test_pageRange_hasCapacity(self):
        pass


    ### ---TEST PAGE DIRECTORY--------------------------------------
    def test_pageDirectory_init(self):
        pass

    def test_pageDirectory_insertBaseRecord(self):
        pass

    def test_pageDirectory_insertTailRecord(self):
        pass

    def test_pageDirectory_addNewPageRange(self):
        pass

    def test_pageDirectory_getPhysicalPages(self):
        pass

    def test_pageDirectory_getRecordCapacity(self):
        pass

if __name__ == "__main__":
    unittest.main()