import copy

from template.config import *
from template.page_range import PageRange  # import for merging
from template.physical_pages import PhysicalPages
from template.table import Table
from template.buffer_pool import BufferPool
from template.MergeThread import MergeThread


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
        self.merge_thread = MergeThread(self, 5)  # sleep for 5 seconds

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
                self.tables.append(Table(table_name, int(num_columns), int(key_column), self.bufferPool))

    def close(self):
        self.bufferPool.save(self.tables)
        self.merge_thread.stop_thread()  # prompts the thread to finish and terminate

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
            raise TableExistsError(name)

        self.bufferPool.currPageRangeIndexes[name] = -1
        self.bufferPool.numOfColumns[name] = num_columns
        table = Table(name, num_columns, key, self.bufferPool)
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

    def merge_placeholder(self):
        """
        Placeholder for the thread to test on

        Delete when db.merge() is ready
        """

        for table in self.tables:
            print("merging on tables: " + table.table_name)

    def merge(self):
        """
        method to merge on some granularity
        """

        # get the table from the bufferpool (which has determined the order)
        table = Table("placeholder", 4, 0, BufferPool()) # placeholder
        range_to_merge = PageRange(4, "placeholder", 0)  # placeholder

        for orig_bp in range_to_merge.basePages:
            # do not copy indirection column, do reference to orig

            # init a new BP instance to consolidate into
            consolidated_BP = PhysicalPages(table.num_columns)

            # non-indirection columns can be copied because they aren't concurrently updated
            # deepcopy() is used to copy an object and child objects within it
            # copy() shallow copies an object but its children are references to the original
            consolidated_BP.numOfRecords = copy.deepcopy(orig_bp.numOfRecords)
            for i in enumerate(consolidated_BP.physicalPages):
                if i != INDIRECTION_COLUMN:
                    consolidated_BP.physicalPages[i] = copy.deepcopy(orig_bp.physicalPages[i])

            # reference the original indirection column to account for concurrent updates
            consolidated_BP.physicalPages[INDIRECTION_COLUMN] = orig_bp.physicalPages[INDIRECTION_COLUMN]

            # TODO: replace data columns in consolidated_BP with latest TP data
            #  Reverse iterate Tail Page records
