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
        self.db_path = ""

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

        # TODO
        self.dirtyBitTracker = []  # keeps track of number of transactions

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
            self.bufferPool.requestPageRange(self.table_name, page_range_index)

        # desired PageRange should be in BufferPool at this point
        return self.bufferPool.get_page_range_from_buffer_pool(
            self.table_name,
            page_range_index
        )

    """
    #adds PageRange to bufferpool under the assumption that there is already a slot open

    :param page_range: filled PageRange()
    """
    def add_page_range_to_buffer_pool(self, page_range):
        selfpageRanges.append(page_range)

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

        page_range = self.read_from_disk(table_index, page_range_index)

        self.add_page_range_to_buffer_pool(page_range)


    """
    # This is called when PageRange they are trying to add a Base Record to is full
    # This will only be called during inserts

    #create a new PageRange on disk
    #TODO: Long
    """

    def addNewPageRangeToDisk(self, table_name):
        self.currPageRangeIndexes[table_name] += 1

        page_range_index = self.currPageRangeIndexes[table_name]

        page_range = PageRange(num_of_cols, page_range_index, table_name)

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
        path = "./disk/" + str(db_name) + "/" + str(table_name) + "/" + "pageRange" + str(
            page_range_index) + ".p"
        return path

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

        file_path = self.get_path(self.db_name, table_name, page_range_index)
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
        table_name = page_range.table_name
        path = self.get_path(self.db_name, table_name, page_range.index)
        fs = open(path, "wb")
        pickle.dump(page_range, fs)
        fs.close()

    """
    - determine which page in the buffer pool to evict
    - check if the evictable page is dirty, if so update the page in disk
    - pinning and unpinning counters -> might have to add that to the Page

    aly
    """

    def remove_LRU_page(self):
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

    def insertTableMetaData(self, table_name, num_columns, page_range_index=0):
        fs = open(self.db_path + "/tables_metadata.txt", "a")
        table_entry = "{0}, {1}, {2}\n".format(table_name, num_columns, page_range_index)
        fs.write(table_entry)
        fs.close()

    def close(self):
        path = os.path.join(self.db_path, "tables_metadata.txt")
        with open(path, "w") as fs:
            table_names = list(self.currPageRangeIndexes.keys())
            for table_name in table_names:
                columns = self.numOfColumns[table_name]
                currPRIndex = self.currPageRangeIndexes[table_name]
                table_entry = "{0}, {1}, {2}\n".format(table_name, columns, currPRIndex)
                fs.write(table_entry)


