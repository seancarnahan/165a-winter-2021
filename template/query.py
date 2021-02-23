from template.table import Table, Record
from template.index import Index
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

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    When a record is deleted, the base record will be
    invalidated by setting the RID of itself and all its tail records to a special value
    """
    def delete(self, key):
        # RID is for the base record
        # schema encoding = 2 for delete
        try:
            rid = self.table.index.locate(self.table.key, key)
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

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try:
            self.table.createNewRecord(columns[0], columns)
            return True
        except Exception as e:
            return False

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
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
        except Exception as e:
            return False


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # rid is the rid of base record
        rids = self.table.index.locate(self.table.key, key)
        try:
            for rid in rids:
                self.table.updateRecord(self.table.key, rid, columns)
            return True
        except:
            return False

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
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


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_all_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_all_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
