from template.config import *
from template.page_range import PageRange
from shutil import rmtree # used to remove directories
from threading import RLock, Condition, Lock
import copy
import os
import pickle


class BufferPool:

    def __init__(self):
        self.size = BUFFER_POOL_NUM_OF_PRs
        self.pageRanges = []  # / list[PageRange(page_range_index, table_name)]
        self.db_path = "./disk"
        self.bp_lock = RLock()
        self.pr_available_to_evict_condition = Condition(self.bp_lock)

        """
        # BELOW: MetaData that persists even after DB is closed
        # table_name, num_columns, currPageRangeIndex, ...
        """
        # this is used for inserts to know the next available pageRange in a table
        # key: table_name
        # value: the available page_range_index for the next insert for the corresponding table
        # DB initializes a value every time a new table gets created
        self.currPageRangeIndexes = {}

        # key: table_name
        # value: numOfColumns for that table
        # DB initializes a value every time a new table gets created
        self.numOfColumns = {}

        # list of [tableName, PR_index_rel_to_table, tailRecordsSinceLastMerge]
        self.tailRecordsSinceLastMerge = []
        # list of [table_name, PR_index_rel_to_table, totalRecords]
        self.recordsInPageRange = []

        """ 
        BELOW: State of current Page Ranges in BufferPool
        """
        # tracks transactions
        self.pins = []

        # count requests per page range
        self.requestsPerPR = []

        # true or false if have been updated
        self.dirtyBitTracker = []

        if not os.path.exists(self.db_path):
            os.mkdir(self.db_path)
    """
    input: None
    Output: [TableName, PageRange]
    
    # if nothing to merge return True 
    """
    def getPageRangeForMerge(self):
        with self.bp_lock:
            # sort the list of lists by 3rd element: tailRecordsSinceLastMerge
            sorted_tailRecordsSinceLastMerge = copy.deepcopy(self.tailRecordsSinceLastMerge)
            sorted_tailRecordsSinceLastMerge.sort(key=lambda x: x[2])

            # Don't need to merge
            if len(sorted_tailRecordsSinceLastMerge) == 0:
                return True

            # get the greatest num of tailRecordsSinceLastMerge
            greastestNumOfTailRecs = sorted_tailRecordsSinceLastMerge[-1]

            # Don't need to do merge if less than 50 tail records
            if greastestNumOfTailRecs[2] <= 50:
                return True

            # get TableName and page range index
            return greastestNumOfTailRecs[0:2]

    """
    get the desired index in tailRecordsSinceLastMerge based off the params
    """
    def get_tailRecordsSinceLastMerge_index(self, table_name, page_range_index):

        with self.bp_lock:
            for i in range(len(self.tailRecordsSinceLastMerge)):
                curr = self.tailRecordsSinceLastMerge[i]

                if curr[0] == table_name and curr[1] == page_range_index:
                    return i
            print(" invalid params on [get_tailRecordsSinceLastMerge_index]")
            return False

    def resetTailPageRecordCount(self, table_name, page_range_index):
        with self.bp_lock:
            index = self.get_tailRecordsSinceLastMerge_index(table_name, page_range_index)

            self.tailRecordsSinceLastMerge[index][2] = 0

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
        with self.bp_lock:
            isPageRangeInBP = False

            # check if pageRange is in BufferPool
            for i in range(len(self.pageRanges)):
                page_range = self.pageRanges[i]

                if page_range.table_name == table_name and page_range.id == page_range_index:
                    isPageRangeInBP = True
                    break

            # PageRange is not currently in BufferPool
            if not isPageRangeInBP:
                # request that desired PageRange gets added to BufferPool
                self.requestPageRange(table_name, page_range_index)

            page_range_index_in_BP = self.get_page_range_index_in_buffer_pool(table_name, page_range_index)

            # increment pins
            self.pins[page_range_index_in_BP] += 1

            # increment requestsPerPR
            self.requestsPerPR[page_range_index_in_BP] += 1

            # desired PageRange should be in BufferPool at this point
            return self.get_page_range_from_buffer_pool( table_name, page_range_index)


    def releasePin(self, table_name, page_range_index):
        with self.pr_available_to_evict_condition:
            #decrement pin
            page_range_index_in_BP = self.get_page_range_index_in_buffer_pool(table_name, page_range_index)
            self.pins[page_range_index_in_BP] -= 1
            if self.pins[page_range_index_in_BP] == 0:
                self.pr_available_to_evict_condition.notify()

    def get_page_range_index_in_buffer_pool(self, table_name, page_range_index):
        with self.bp_lock:
            for i in range(len(self.pageRanges)):
                pageRange = self.pageRanges[i]

                if pageRange.table_name == table_name and pageRange.id == page_range_index:
                    return i

    """
    # adds PageRange to bufferpool under the assumption that there is already a slot open

    :param page_range: filled PageRange()
    """
    def add_page_range_to_buffer_pool(self, page_range):
        with self.bp_lock:
            # init a pin count
            self.pins.append(0)

            # init a request per page range count
            self.requestsPerPR.append(0)

            # add a dirty bit to dirty bit tracker
            self.dirtyBitTracker.append(False)

            # add page range to bufferpool
            self.pageRanges.append(page_range)

    """
    # this gets called only when the desired page range is not in the bufferPool
    # so we remove LFU pageRange -> only remove LFU when there are 3 in memory(use config)
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
    #TODO: make this async
    def requestPageRange(self, table_name, page_range_index):
        with self.bp_lock:
            if len(self.pageRanges) >= BUFFER_POOL_NUM_OF_PRs:
                """
                # python is inherently synchronous by nature: However,
                # if this function never finds a page range to remove this will
                # loop on forever -> could potentially cause issues
                """
                self.remove_LFU_page()

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
    """
    def addNewPageRangeToDisk(self, table_name):
        with self.bp_lock:
            self.currPageRangeIndexes[table_name] += 1

            num_of_cols = self.numOfColumns[table_name]
            page_range_index = self.currPageRangeIndexes[table_name]

            self.tailRecordsSinceLastMerge.append([table_name, page_range_index, 0])
            self.recordsInPageRange.append([table_name, page_range_index, 0])

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
        with self.bp_lock:
            for page_range in self.pageRanges:
                if page_range.table_name == table_name and page_range.id == page_range_index:
                    return page_range

    """
    :param table_name: name of the table

    :return the latest PageRange created for the table
    """
    def getCurrPageRangeIndex(self, table_name):
        with self.bp_lock:
            return self.currPageRangeIndexes[table_name]

    """
    Read Page Range from disk and bring into memory
    """
    def read_from_disk(self, table_name: str, page_range_index: int):  # Gabriel
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
        table_name = page_range.table_name
        path = self.get_path(self.db_path, table_name, page_range.id)
        fs = open(path, "wb")
        pickle.dump(page_range, fs)
        fs.close()

    def remove_LFU_page(self):

        with self.pr_available_to_evict_condition:
            # find least recently used pageRanges
            ordered_LFUs = self.order_LFUs()
            isRemoved = False

            while not isRemoved:
                for i in range(len(ordered_LFUs)):
                    if self.check_if_pr_not_in_use(ordered_LFUs[i]):
                        # remove page range
                        self.removePageRangeFromBufferPool(ordered_LFUs[i])
                        isRemoved = True
                        break
                self.pr_available_to_evict_condition.wait()

    """
    :param index: the index of the page range in buffer pool
    
    check dirty bit
    """
    def removePageRangeFromBufferPool(self, index):

        with self.bp_lock:
            #check for a dirty bit, if true, write to disk
            if self.dirtyBitTracker[index] == True:
                self.write_to_disk(self.pageRanges[index])

            try:
                # remove page range
                del self.pageRanges[index]

                # remove dirtyBitTracker
                del self.dirtyBitTracker[index]

                # remove requestsPerPR
                del self.requestsPerPR[index]

                # remove pin
                del self.pins[index]
            except IndexError:
                print("ERROR in [removePageRangeFromBufferPool]: invalid index")

    """
    list [least recently used -> most used]
    
    return list[index of page ranges in buffer pool, but are sorted]
    """
    def order_LFUs(self):
        # requestsPerPRCopy = requestsPerPR.copy()

        mappedList = [[requests, idx] for idx, requests in enumerate(self.requestsPerPR)]
        mappedList.sort(key=lambda tup: tup[0])

        page_ranges_sorted_by_LFU = list(map(lambda tup: tup[1], mappedList))

        return page_ranges_sorted_by_LFU

    """
    :param index: the index of the page range in buffer pool
    
    return true if the page range is NOT in use, false otherwise
    """
    def check_if_pr_not_in_use(self, index):
        pin = self.pins[index]

        if pin == 0:
            return True
        else:
            return False

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
            fs = open(os.path.join(self.db_path, "page_range_metadata.txt"), "w")
            fs.close()

    def save(self, tables):
        headers = "table_name,num_columns,key_column,currentPRIndex\n"
        path = os.path.join(self.db_path, "tables_metadata.txt")
        with open(path, "w") as fs:
            fs.write(headers)
            for table in tables:
                table_name = table.table_name
                key_column = table.key
                currPRIndex = self.currPageRangeIndexes[table_name]
                table_entry = "{0},{1},{2},{3}\n".format(table_name, table.num_columns, key_column, currPRIndex)
                fs.write(table_entry)

        path = os.path.join(self.db_path, "page_range_metadata.txt")
        with open(path, "w") as fs:
            for entry in self.tailRecordsSinceLastMerge:
                line = "{0},{1},{2}\n".format(*entry)
                fs.write(line)

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

        path = os.path.join(self.db_path, "page_range_metadata.txt")
        with open(path) as fs:
            for line in fs:
                table_name, pr_index, tailRecordsRemaining = line.split(",")
                self.tailRecordsSinceLastMerge.append([table_name, int(pr_index), int(tailRecordsRemaining)])