from template.table import Table
from template.buffer_pool import BufferPool


class TableExistsError(Exception):
    def __init__(self, table):
        super().__init__("Table exists: {0}".format(table))

class Database:

    def __init__(self):
        self.tables = []
        self.bufferPool = BufferPool()

    def open(self, path):
        self.bufferPool.db_path = path
        self.bufferPool.createDatabaseDirectory()
        pass

    def close(self):
        self.bufferPool.close()
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

        table = Table(name, num_columns, key, self.bufferPool)
        self.tables.append(table)
        self.bufferPool.currPageRangeIndexes[name] = 0
        self.bufferPool.numOfColumns[name] = num_columns

        return table

    """
    # Deletes the specified table
    """

    def drop_table(self, name):
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)
                self.bufferPool.deleteTableDirectory(name)

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
