from template.config import *
from template.page import Page
import sys


class BufferPool:
    def __init__(self):
        self.size = BUFFER_POOL_NUM_BASE_PAGES
        self.TPsSize = BUFFER_POOL_NUM_TAIL_PAGES_PER_BASE
        self.basePages = []

        # key: index on self.basePages
        # value: list of corresponding tail pages
        self.tailPages = {}

    """
        input: [table index, type, PR Index, BP/TP index]
        - input should all be strings
        output: true or false 

        sean
    """

    def create_file(self, table_index: str, pageType: str, pr_index: str, _P_index: str):
        fileName = "../disk/" + table_index + pageType + pr_index + _P_index + ".txt"

        page = open(fileName, "x")

    """
        updates: when we close the DB or evict a dirty page
        updates the txt file
        returns true if its able to update; else: false

        long
        
    """

    def update_page(self):
        pass

    def read_from_disk(self, page_file_name):  # Gabriel
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
        return name of file created

        sean
    """

    def write_to_disk(self):
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
