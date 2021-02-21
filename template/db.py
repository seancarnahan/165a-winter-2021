from template.table import Table
from template.buffer_pool import BufferPool
from template.MergeThread import MergeThread


class TableExistsError(Exception):
    def __init__(self, table):
        super().__init__("Table exists: {0}".format(table))


class Database:

    def __init__(self):
        self.tables = []
        self.bufferPool = BufferPool()
        self.merge_thread = MergeThread(self, 5)  # sleep for 5 seconds

    def open(self, path):
        self.bufferPool.setDatabaseLocation(path)
        self.bufferPool.createDatabaseDirectory()
        self.merge_thread.thread_start()
        pass

    def close(self):
        self.bufferPool.save()
        self.merge_thread.stop_thread()  # prompts the thread to finish and terminate
        pass

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

        raise Exception("table: " + name + " not found in database")

    """
    input:
    output: loads to page to buffer pool
    """
    def fetchPage(self):
        pass

    def merge_placeholder(self):

        for table in self.tables:
            print("merging on tables: " + table.table_name)
