from template.table import Table
from template.buffer_pool import BufferPool

class Database:

    def __init__(self):
        self.tables = []
        self.bufferPool = BufferPool()
        self.next_table_index = 0

    def open(self, path):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.bufferPool, next_table_index)
        self.tables.append(table)

        self.next_table_index += 1
        self.bufferPool.currPageRangeIndexes.append(0)

        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)

    """
    # Returns table with the passed name
    """

    def get_table(self, name):
        for table in self.tables:
            if table.name == name:
                return table

        raise Exception("table: " + name + " not found in database")


    """
    input: 
    output: loads to page to buffer pool
    """
    def fetchPage(self):
        pass