from template.config import *
from template.page_range import PageRange
from shutil import rmtree # used to remove directories
import sys
import os
import pickle

class BufferPool:

    def __init__(self):
        self.size = BUFFER_POOL_NUM_OF_PRs
        self.pageRanges = []  # / list[PageRange(page_range_index, table_name)]
        self.db_path = "./disk"

        # metadata to update
        """
        this is used for inserts to know the next available pageRange in a table

        key: table_name
        value: the available page_range_index for the next insert for the corresponding table

        *DB initializes a value every time a new table gets created
        """
        self.currPageRangeIndexes = {}
        #table_name, num_columns, currPageRangeIndex

        """
        key: table_name
        value: numOfColumns for that table

        *DB initializes a value every time a new table gets created
        """
        self.numOfColumns = {}

        # tracks transactions
        self.pins = []

        #increment counter everytime load is called on this
        self.requestsPerPR = [] #counter for requests

        self.dirtyBitTracker = []  # keeps track of number of transactions

        if not os.path.exists(self.db_path):
            os.mkdir(self.db_path)

    def setDatabaseLocation(self, path: str):
        if path[0:2] == "./":
            path = path[2:]
        self.db_path = os.path.join(self.db_path, path)

    def readMetadata(self):
        path = os.path.join(self.db_path, "tables_metadata.txt")
        if os.path.exists(path):
            with open(path, "r") as fs:
                row = fs.readline()
                if len(row) == 0:
                    return
                table_name, num_columns, page_range_index = row.split(",")
                self.currPageRangeIndexes[table_name] = page_range_index
                self.numOfColumns[table_name] = num_columns

    """
    1. checks to see if the pageRange is in the table, if its not then it triggers requestPageRange
    2. Once the correct PageRange is loaded then it returns the Page Range from BufferPool

    # returns the desired PageRange from BufferPool
    * might run into async/await issue here
    """

    def loadPageRange(self, table_name, page_range_index):
        isPageRangeInBP = False

        # check if pageRange is in BufferPool
        for i in range(len(self.pageRanges)):
            page_range = self.pageRanges[i]

            if page_range.tableName == table_name and page_range.id == page_range_index:
                isPageRangeInBP = True
                break

        # PageRange is not currently in BufferPool
        if not isPageRangeInBP:
            # request that desired PageRange gets added to BufferPool
            self.requestPageRange(table_name, page_range_index)

        #increment pins
        page_range_index_in_BP = get_page_range_index_in_buffer_pool(table_name, page_range_index)
        self.pins[page_range_index_in_BP] += 1

        # desired PageRange should be in BufferPool at this point
        return self.get_page_range_from_buffer_pool( table_name, page_range_index)


    def unloadPageRange(self, table_name, page_range_index):
        #decrement pin
        page_range_index_in_BP = get_page_range_index_in_buffer_pool(table_name, page_range_index)
        self.pins[page_range_index_in_BP] -= 1

    def get_page_range_index_in_buffer_pool(self, table_name, page_range_index):
        for i in range(len(self.pageRanges)):
            pageRange = self.pageRanges[i]

            if pageRange.tableName == table_name and pageRange.id == page_range_index:
                return i

    """
    #adds PageRange to bufferpool under the assumption that there is already a slot open

    :param page_range: filled PageRange()
    """
    def add_page_range_to_buffer_pool(self, page_range):
        self.pageRanges.append(page_range)

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
        if len(self.pageRanges) >= BUFFER_POOL_NUM_OF_PRs:
            self.remove_LRU_page()

        try:
            page_range = self.read_from_disk(table_name, page_range_index)
        except FileNotFoundError as e:
            self.addNewPageRangeToDisk(table_name)
            page_range = self.read_from_disk(table_name, page_range_index)

        self.add_page_range_to_buffer_pool(page_range)


    """
    # This is called when PageRange they are trying to add a Base Record to is full
    # This will only be called during inserts

    #create a new PageRange on disk
    #TODO: Long
    """

    def addNewPageRangeToDisk(self, table_name):

        self.currPageRangeIndexes[table_name] += 1

        num_of_cols = self.numOfColumns[table_name]
        page_range_index = self.currPageRangeIndexes[table_name]

        page_range = PageRange(num_of_cols, table_name, page_range_index)

        # add page Range to buffer pool
        self.write_to_disk(page_range)

    """
    :param db_name: name of the DB
    :param table_name: name of the table
    :param page_range_index: index of the page range in table

    output: creates str relative path to file in the following format:
        "../disk/db_name/table_name/page_range_name_with_index.p"

    TODO: long
    """

    def get_path(self, db_name, table_name, page_range_index):
        return os.path.join(db_name, table_name, "pageRange{0}.p".format(page_range_index))

    """
    # assume the correct PageRange is already in buffer pool
    # returns the PageRange from bufferPool

    :param table_name: name of the table
    :param page_range_index: index of the page range in table
    # output: PageRange()

    TODO: long
    """

    def get_page_range_from_buffer_pool(self, table_name, page_range_index):
        for page_range in self.pageRanges:
            if page_range.tableName == table_name and page_range.id == page_range_index:
                return page_range

    """
    :param table_name: name of the table

    :return the latest PageRange created for the table
    """

    def getCurrPageRangeIndex(self, table_name):
        return self.currPageRangeIndexes[table_name]


    def read_from_disk(self, table_name: str, page_range_index: int):  # Gabriel
        """
        Read Page Range from disk and bring into memory
        """

        file_path = self.get_path(self.db_path, table_name, page_range_index)
        fs = open(file_path, "rb")
        page = pickle.load(fs)
        fs.close()
        return page

    """
        input: Page Range Object
        updates: when we close the DB or evict a dirty page
        updates the txt file
        returns true if its able to update; else: false
    """

    def write_to_disk(self, page_range: PageRange):
        table_name = page_range.tableName
        path = self.get_path(self.db_path, table_name, page_range.id)
        fs = open(path, "wb")
        pickle.dump(page_range, fs)
        fs.close()

    """
    - determine which page in the buffer pool to evict
    - check if the evictable page is dirty, if so update the page in disk
    - pinning and unpinning counters -> might have to add that to the Page

    aly
    """

    #must return something, make sure to await on this function
    def remove_LRU_page(self):
        #keep looping until a page is removed
        while True:
            # find least recently used pageRanges
            ordered_LRUs = self.order_LRUs()

            for i in range(len(ordered_LRUs)):
                if check_if_pr_not_in_use():
                    #remove page range
                    #reset pin
                    pass
            # if all are in use start cycle over again

    """
    :param index: the index of the page range in buffer pool
    
    check for index out of bounds error
    check dirty bit
    """
    def removePageRangeFromBufferPool(self, index):
        pass

    """
    return list [least recently used -> most used]
    """
    def order_LRUs(self):

        mappedList = list(map(lambda x: (x, self.requestsPerPR.index(x)), self.requestsPerPR))

        mappedList.sort(key=lambda tup: tup[0])

        page_ranges_sorted_by_LFU = list(map(lambda tup: tup[0], mappedList))

        return page_ranges_sorted_by_LFU

    """
    return true if the page range is NOT in use, false otherwise
    """
    def check_if_pr_not_in_use(self):
        pass

    def createTableDirectory(self, table_name):
        full_path = os.path.join(self.db_path, table_name)
        os.mkdir(full_path)

    def deleteTableDirectory(self, table_name):
        full_path = os.path.join(self.db_path, table_name)
        rmtree(full_path, ignore_errors=True)

    def createDatabaseDirectory(self):
        if not os.path.exists(self.db_path):
            os.mkdir(self.db_path)
            fs = open(os.path.join(self.db_path, "tables_metadata.txt"), "w")
            fs.close()

    def save(self, tables):
        headers = "table_name, num_columns, key_column, currentPRIndex\n"
        path = os.path.join(self.db_path, "tables_metadata.txt")
        with open(path, "w") as fs:
            fs.write(headers)
            for table in tables:
                table_name = table.table_name
                key_column = table.key
                currPRIndex = self.currPageRangeIndexes[table_name]
                table_entry = "{0},{1},{2},{3}\n".format(table_name, table.num_columns, key_column, currPRIndex)
                fs.write(table_entry)

        for page_range in self.pageRanges:
            self.write_to_disk(page_range)

    def load_data(self):
        path = os.path.join(self.db_path, "tables_metadata.txt")

        with open(path) as fs:
            fs.readline()
            for table_entry in fs:
                table_name, num_columns, _, currentPRIndex = table_entry.split(",")

                self.numOfColumns[table_name] = int(num_columns)
                self.currPageRangeIndexes[table_name] = int(currentPRIndex)


