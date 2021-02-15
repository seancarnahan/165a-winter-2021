from template.table import Table
from template.buffer_pool import BufferPool

class Database:

    def __init__(self):
        self.tables = []
        self.bufferPool = BufferPool()

    def open(self, path):
        pass

    def close(self):
        pass

    def create_table(self, name, num_columns, key):
        """
        Creates a new table

        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns

        :returns: The newly created table object
        """
        table = Table(name, num_columns, key, self.bufferPool)
        self.tables.append(table)
        return table

    def drop_table(self, name):
        """
        Deletes the specified table

        :param name: string         #Table Name
        """
        for table in self.tables:
            if table.name == name:
                self.tables.remove(table)

    def get_table(self, name):
        """
        Returns the table with the passed name

        :param name: string         #Table Name

        :returns: Table object corresponding to name.
        """
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