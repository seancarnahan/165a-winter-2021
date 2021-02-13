from template.config import *
import sys

class BufferPool:
    def __init__(self):
        self.size = BUFFER_POOL_NUM_BASE_PAGES
        self.TPsSize = BUFFER_POOL_NUM_TAIL_PAGES_PER_BASE
        self.basePages = []

        #key: index on self.basePages
        #value: list of corresponding tail pages
        self.tailPages = {}
    

    """
        input: [table index, type, PR Index, BP/TP index]
        output: true or false 

        sean
    """
    def create_file(self):
        pass

    """
        updates: when we close the DB or evict a dirty page
        updates the txt file
        returns true if its able to update; else: false

        long
        
    """
    def update_page(self):
        pass

    """
        input: fileName
        return new page object

        gabriel
    """
    def read_from_disk(self):
        pass

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







#buffer pool writes and reads to disk
#pageDir keeps trask of our structure of disk ???

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