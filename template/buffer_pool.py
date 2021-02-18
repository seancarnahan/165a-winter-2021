from template.config import *
from template.page import Page
import sys
import os

class BufferPool:

    def __init__(self):
        self.size = BUFFER_POOL_NUM_OF_PRs
        self.pageRanges = [] #/ list[PageRange()]
        #pass down db name

        #metadata to update
        """
        index: table_index
        value: the available page_range_index for the next insert for the corresponding table

        *DB initializes a value every time a new table gets created
        """
        self.currPageRangeIndexes = [] #this should be a map

        #TODO
        self.dirtyBitTracker = [] #keeps track of number of transactions

    """
    this gets called when the desired page range is not in the bufferPool
    so we remove LRU pageRange -> only remove LRU when there are 3 in memory(use config)
    then go to disk read in a Page Range Object based off the params
    then add this new pageRange to the bufferpool

    # return a Page Range to load into the bufferpool
    # talks to disk and then sets the pageRange of BufferPool
    :param fileName: path to the file in disk that holds the desired pageRange

    return null

    long
    """
    #TODO
    def requestPageRange(self, table_index, page_range_index):
        #if not Page Ranges have been created we should then create a new one
        pass



    #TODO
    """
    brand new PageRange -> used for inserts
    """
    def addNewPageRange(self, table_index):
        #create a new PageRange on disk
        #load new PageRange into bufferpool
        #notify PageDirectory that a new PageRange has been loaded?

        self.currPageRangeIndexes[table_index] += 1
        pass

    """
    output: str relative path to file

    "../disk/db_name/table_name/page_range_name.p"

    long
    """
    def get_path(self, db_name, table_name, page_range_index):
        pass

    """
        assume the correct page is already in buffer pool
        output: PageRange()

        long
    """
    def get_page_range_from_buffer_pool(self, table_name, page_range_index):
        pass

    # def getPageRange(self):
    #     return self.pageRange

    def getCurrPageRangeIndex(self, table_index):
        return self.currPageRangeIndexes[table_index]


    """
        input: [table index, type, PR Index, BP/TP index]
        - input should all be strings
        output: fileName

        sean
    """
    def create_file(self, table_index: str, pageType: str, pr_index: str, _P_index: str):
        fileName = "../disk/" + table_index + pageType + pr_index + _P_index + ".txt"

        try:
            page = open(fileName, "x")
        except Exception as e:
            print(e)

        page.close()
f
        return fileName

    """
        input: filename (relative path?)
        output: true or false based on succesful deletion
    """
    def deleteFile(self, fileName: str):
        pass


    def read_from_disk(self, table_index, page_range_index):  # Gabriel
        """
        Read data from disk

        :param page_file_name: string       # The file name corresponding to the page we want to load
        """

        # I don't think we decided on a file format so this code is mainly temporary

        page = Page()
        with open(page_file_name, 'r') as fs:
            numRecords = int(fs.readline())
            data = bytearray()
            data += fs.read()

        page.num_records = numRecords
        page.data = data

        return page

    """
        input: page Object/ byteArray
        updates: when we close the DB or evict a dirty page
        updates the txt file
        returns true if its able to update; else: false

        long
    """

    def write_to_disk(self, table_index, page_range_index):
        pass

    """
        - determine which page in the buffer pool to evict
        - check if the evictable page is dirty, if so update the page in disk
        - pinning and unpinning counters -> might have to add that to the Page

        aly
    """
    def remove_LRU_page(self):
        pass



# buffer pool writes and reads to disk
# pageDir keeps trask of our structure of disk ???

"""
    size = BUFFER_POOL_SIZE
    path = None
    # active pages loaded in bufferpool
    page_directories = {}
    # Pop the least freuqently used page
    tstamp_directories = {}
    tps = {}  # Key: (table_name, col_index, page_range_index), value: tps
    latest_tail = {}  # Key: (table_name, col_index, page_range_index), value: lastest tail page id of specified page range
    def __init__(self):
        # print("Init BufferPool. Do Nothing ...")
        pass
"""
