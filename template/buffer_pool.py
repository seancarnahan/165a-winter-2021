from template.config import *
from template.page import Page
import sys
import os

class BufferPool:

    def __init__(self, path_to_db_name):
        self.size = BUFFER_POOL_NUM_OF_PRs
        self.pageRanges = [] #/ list[PageRange(page_range_index, table_name)]
        self.path_to_db_name = path_to_db_name

        #metadata to update
        """
        this is used for inserts to know the next available pageRange in a table

        key: table_name
        value: the available page_range_index for the next insert for the corresponding table

        *DB initializes a value every time a new table gets created
        """
        self.currPageRangeIndexes = {}

        #TODO
        self.dirtyBitTracker = [] #keeps track of number of transactions

    """
    1. checks to see if the pageRange is in the table, if its not then it triggers requestPageRange
    2. Once the correct PageRange is loaded then it returns the Page Range from BufferPool

    # returns the desired PageRange from BufferPool
    * might run into async/await issue here
    """
    def loadPageRange(self, table_name, page_range_index):
        isPageRangeInBP = False

        #check if pageRange is in BufferPool
        for i in range(len(self.pageRanges)):
            page_range = self.pageRanges[i]

            if page_range.tableName == table_name and page_range.id == page_range_index:
                isPageRangeInBP = True
                break

        #PageRange is not currently in BufferPool
        if not isPageRangeInBP:
            #request that desired PageRange gets added to BufferPool
            self.bufferPool.requestPageRange(self.table_name, page_range_index)

        #desired PageRange should be in BufferPool at this point
        return self.bufferPool.get_page_range_from_buffer_pool(
            self.table_name,
            page_range_index
        )

    """
    # this gets called only when the desired page range is not in the bufferPool
    # so we remove LRU pageRange -> only remove LRU when there are 3 in memory(use config)
    # then go to disk read in a Page Range Object based off the params
    # then add this new pageRange to the bufferpool

    # return a Page Range to load into the bufferpool
    # talks to disk and then sets the pageRange of BufferPool
    :param table_name: name of the table
    :param page_range_index: index of the page range in table

    # don't need to return anything
    # if PageRange is not in disk then throw error
    # might utilize get_path()

    # TODO: Long
    """
    def requestPageRange(self, table_name, page_range_index):
        pass


    """
    # This is called when PageRange they are trying to add a Base Record to is full
    # This will only be called during inserts

    #create a new PageRange on disk
    #load new PageRange into bufferpool -> might want to call requestPageRange

    #TODO: Long
    """
    #TODO
    def addNewPageRange(self, table_name):
        self.currPageRangeIndexes[table_name] += 1
        pass

    """
    :param db_name: name of the DB
    :param table_name: name of the table
    :param page_range_index: index of the page range in table

    output: creates str relative path to file in the following format:
        "../disk/db_name/table_name/page_range_name_with_index.p"

    TODO: long
    """
    def get_path(self, db_name, table_name, page_range_index):
        pass

    """
    # assume the correct PageRange is already in buffer pool
    # returns the PageRange from bufferPool

    :param table_name: name of the table
    :param page_range_index: index of the page range in table
    # output: PageRange()

    TODO: long
    """
    def get_page_range_from_buffer_pool(self, table_name, page_range_index):
        pass

    """
    :param table_name: name of the table

    :return the latest PageRange created for the table
    """
    def getCurrPageRangeIndex(self, table_name):
        return self.currPageRangeIndexes[table_name]


    """
    SUBJECT TO CHANGE

    input: [table index, type, PR Index, BP/TP index]
    - input should all be strings
    output: fileName
    """
    def create_file(self, table_name: str, pageType: str, pr_index: str, _P_index: str):
        #this method needs to change
        # fileName = "../disk/" + table_name + pageType + pr_index + _P_index + ".txt"

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


    def read_from_disk(self, table_name, page_range_index):  # Gabriel
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

    def write_to_disk(self, table_name, page_range_index):
        pass

    """
    - determine which page in the buffer pool to evict
    - check if the evictable page is dirty, if so update the page in disk
    - pinning and unpinning counters -> might have to add that to the Page

    aly
    """
    def remove_LRU_page(self):
        pass
