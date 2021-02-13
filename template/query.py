from template.config import *


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    def delete(self, key):
        """
        INTERNAL METHOD!!
        Deletes record with matching key.

        :param key: Int.        The primary key of a record.

        :returns: True upon successful deletion.
                  False if record doesn't exist or is locked due to 2PL
        """

        # When a record is deleted, the base record will be invalidated by creating a new tail record with
        # schema encoding set as 2.
        try:
            rid = self.table.index.locate(self.table.key, key) # base record rid
        except IndexError:
            return False

        values = []
        for value in range(self.table.num_all_columns - RECORD_COLUMN_OFFSET):
            values.append(0)
        try:
            self.table.updateRecord(key, rid[0], values, deleteFlag=True)
            return True
        except:
            return False

    def insert(self, *columns):
        """
        Insert a record with specified columns

        :param columns: data to be inserted into the table.
        :returns: True if insert operation is successful. Otherwise False
        """
        try:
            self.table.createNewRecord(columns[0], columns)
            return True
        except:
            return False

    def select(self, key, column, query_columns):
        """
        Read a record with specified key. Assume this function will never be called on a key
        that doesn't exist.

        :param key: Int.            The key value to select records based on
        :param column: Int.         The column to search for key
        :param query_columns: list of 1s or 0s.  The columns to return.

        :returns: List of record objects upon success. Otherwise False
        """

        rids = self.table.index.locate(column, key)
        recordList = []
        try:
            for rid in rids:
                record = self.table.getLatestupdatedRecord(rid)
                counter = 0
                for bit in query_columns:
                    counter += 1
                    if bit == 0:
                        record.columns[counter - 1] = None
                    recordList.append(record)
            return recordList
        except:
            return False

    def update(self, key, *columns):
        """
        Update a record with specified key and columns

        :param key: Int.            The primary key of record to update.
        :param columns: Tuple.      The column values to update. If entry is None, then it will not be changed.

        :returns: True if update is successful. False if no records exist with given key or if target record cannot
                  be accessed due to 2PL Lockings.
        """
        # rid is the rid of base record
        rids = self.table.index.locate(self.table.key, key)
        try:
            for rid in rids:
                self.table.updateRecord(self.table.key, rid, columns)
            return True
        except:
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        Sums the columns specified by aggregate_column_index. Only sums records between [start_range, end_range].

        :param start_range: int         # Start of the key range to aggregate
        :param end_range: int           # End of the key range to aggregate
        :param aggregate_column_index: int          # Desired column to aggregate. Should have an Index created
                                                      beforehand.

        :returns: The summation of the given range upon success. Otherwise False if no records exists in given range.
        """
        ridRange = self.table.index.locate_range(start_range, end_range,
                                                 self.table.key)

        if ridRange == []:
            return False
        sum = 0
        for rid in ridRange:
            record = self.table.getLatestupdatedRecord(rid)
            value = record.columns[aggregate_column_index]
            sum += value
        return sum

    def increment(self, key, column):
        """
        Incremented one column of the record (Note: Should work if Select & Update work)

        :param key: int         # primary key of record to increment
        :param column: int      # column to increment

        :returns: True if increment is successful. False if no record matches key or if target record is locked by 2PL.
        """
        r = self.select(key, self.table.key, [1] * self.table.num_all_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_all_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False


