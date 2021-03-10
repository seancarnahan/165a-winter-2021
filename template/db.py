import copy

from template.MergeThread import MergeThread
from template.buffer_pool import BufferPool
from template.config import *
from template.physical_pages import PhysicalPages
from template.table import Table


class TableExistsError(Exception):
    def __init__(self, table):
        super().__init__("Table exists: {0}".format(table))


class TableNotFoundError(Exception):
    def __init__(self, table_name):
        super().__init__("Table does not exist: {0}".format(table_name))


class Database:

    def __init__(self):
        self.tables = []
        self.bufferPool = BufferPool()
        self.merge_thread = MergeThread(self, 1)  # sleep for 1 seconds; note: sleep is disabled

    def open(self, path):
        self.bufferPool.setDatabaseLocation(path)
        self.bufferPool.createDatabaseDirectory()
        self.bufferPool.load_data()
        self.__load_tables()
        self.merge_thread.thread_start()

    def __load_tables(self):
        table_metadata_path = self.bufferPool.db_path + "/tables_metadata.txt"

        with open(table_metadata_path) as f:
            f.readline()  # read the headers, we won't need them.
            for table_entry in f:
                table_name, num_columns, key_column, _ = table_entry.split(",")
                self.tables.append(Table(self, table_name, int(num_columns), int(key_column), self.bufferPool))

    def close(self):
        self.merge_thread.stop_thread()  # prompts the thread to finish and terminate
        self.bufferPool.save(self.tables)

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        try:
            self.bufferPool.createTableDirectory(name)
        except FileExistsError:
            raise TableExistsError(name) from None

        self.bufferPool.currPageRangeIndexes[name] = -1
        self.bufferPool.numOfColumns[name] = num_columns
        table = Table(self, name, num_columns, key, self.bufferPool)
        self.tables.append(table)

        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        for table in self.tables:
            if table.table_name == name:
                self.tables.remove(table)
                self.bufferPool.deleteTableDirectory(name)

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        for table in self.tables:
            if table.table_name == name:
                return table
        raise TableNotFoundError(name)

    """
    input:
    output: loads to page to buffer pool
    """

    def fetchPage(self):
        pass

    def merge(self, table_name: str, page_range_index: int):
        """
        method to merge on some granularity

        :type table_name: str
        :param table_name: The Table in which the merge is performed
        :type page_range_index: int
        :param page_range_index: The index of the PageRange whose pages are merging
        """

        # get the table from the buffer_pool (which has determined the order)
        range_to_merge = self.bufferPool.read_from_disk(table_name=table_name, page_range_index=page_range_index)
        merge_table = self.get_table(table_name)  # for getting base records

        '''
        Step: Copy the Base Pages
        do not copy indirection column, do reference to orig
        the indirection column must be the same reference to ensure a contention-free solution
        '''
        for i, orig_bp in enumerate(range_to_merge.basePages):

            # init a new BP instance to consolidate into
            consolidated_BP = PhysicalPages(range_to_merge.num_columns)

            # non-indirection columns can be copied because they aren't concurrently updated
            # deepcopy() is used to copy an object and child objects within it
            # copy() shallow copies an object but its children are references to the original
            consolidated_BP.numOfRecords = copy.deepcopy(orig_bp.numOfRecords)
            for j in range(len(consolidated_BP.physicalPages)):
                if j != INDIRECTION_COLUMN:
                    consolidated_BP.physicalPages[j] = copy.deepcopy(orig_bp.physicalPages[j])

            # reference the original indirection column to account for concurrent updates
            consolidated_BP.physicalPages[INDIRECTION_COLUMN] = orig_bp.physicalPages[INDIRECTION_COLUMN]

            # replace the old base page with the new one
            range_to_merge.basePages[i] = consolidated_BP

        # all base pages should be copies now

        '''
        Step: Overwrite the new Base Pages data
        '''
        # Set for hashing tail records with same BaseRID, no duplicates
        seenUpdatesSet = set()

        # TODO: only reverse iterate until reaching the latest merged tail record
        #  not all the tail records
        # reverse iterate over all tail pages in Page Range
        for item in reversed(list(enumerate(range_to_merge.tailPages))):
            page_num = item[0]
            tail_page = item[1]
            tail_page_records = tail_page.getAllRecords()

            # reverse iterate over all records within this tail page
            for i, record in enumerate(reversed(tail_page_records)):
                if not record[BASE_RID_COLUMN] in seenUpdatesSet:
                    # add the BaseRID as the key to the "hashmap"
                    seenUpdatesSet.add(record[BASE_RID_COLUMN])

                    # get the locations of the new Base Pages and records
                    location_list = merge_table.page_directory.getRecordLocation(record[BASE_RID_COLUMN])
                    base_index = location_list[3] % 1000
                    base_page_index = location_list[2]

                    # self is new base record, change BaseRID accordingly
                    record[BASE_RID_COLUMN] = record[RID_COLUMN]
                    range_to_merge.basePages[base_page_index].replaceRecord(base_index, record)

        # finished iterating backwards over the tail pages, base pages are now consolidated
        # reset the tailRecordsSinceLastMerge counter for this PageRange
        self.bufferPool.resetTailPageRecordCount(table_name, page_range_index)
