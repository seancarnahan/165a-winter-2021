from template.table import Table
from template.buffer_pool import BufferPool


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

    def open(self, path):
        self.bufferPool.setDatabaseLocation(path)
        self.bufferPool.createDatabaseDirectory()
        self.bufferPool.load_data()
        self.__load_tables()

    def __load_tables(self):
        table_metadata_path = self.bufferPool.db_path + "/tables_metadata.txt"

        with open(table_metadata_path) as f:
            f.readline() # read the headers, we won't need them.
            for table_entry in f:
                table_name, num_columns, key_column, _ = table_entry.split(",")
                self.tables.append(Table(table_name, int(num_columns), int(key_column), self.bufferPool))


    def close(self):
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
