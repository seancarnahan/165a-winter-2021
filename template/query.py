from template.config import *
from template.query_result import QueryResult
from template.table import Table


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table: Table):
        self.table = table


    """
    # internal Method
    # Read a record with specified RID
    # Return Query Result upon successful upon successful deletion
        # is_successful = True
        # key = key
        # column_data = (*columns): List
    # Return Query Result upon Failed deletion or if record doesn't exist or is locked due to 2PL
        # is_successful = False
        # key = key
        # column_data = "none"
    # When a record is deleted, the base record will be
    # invalidated by setting the RID of itself and all its tail records to a special value
    # LOCK occurs inside the table on the record and PageRange
    """
    def delete(self, key):
        query_result = QueryResult()
        query_result.set_key(key)

        # RID is for the base record
        # schema encoding = 2 for delete
        try:
            rid = self.table.index.locate(self.table.key, key)
        except IndexError:
            return query_result

        values = []
        for value in range(self.table.num_all_columns - RECORD_COLUMN_OFFSET):
            values.append(0)
        try:
            prevRecordColData = self.table.updateRecord(key, rid[0], values, deleteFlag=True)

            query_result.set_column_data(prevRecordColData)
            query_result.set_is_successful(True)
            return query_result
        except Exception as e:
            return query_result

    """
    # Insert a record with specified columns
    # Return Query Result upon successful insertion
        # is_successful = True
        # key = columns[0]
        # column_data = "none"
    # Return Query Result upon Failed insertion  for whatever reason
        # is_successful = False
        # key = columns[0]
        # column_data = "none"
    # LOCK occurs inside the table on the record and PageRange
    """
    def insert(self, *columns):
        query_result = QueryResult()
        query_result.set_key(columns[self.table.key])

        try:
            self.table.createNewRecord(columns[self.table.key], columns)

            query_result.set_is_successful(True)
            return query_result
        except Exception as e:
            return query_result

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    # LOCK occurs on the record from Query class
    """    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    # LOCK occurs on the record from Query class
    """

    def select(self, key, column, query_columns):
        query_result = QueryResult()
        rids = self.table.index.locate(column, key)

        recordList = []
        try:
            for rid in rids:
                # get the record
                record = self.table.getLatestupdatedRecord(rid)

                counter = 0
                for bit in query_columns:
                    counter += 1
                    if bit == 0:
                        record.columns[counter - 1] = None
                recordList.append(record)

            query_result.set_key(key)
            query_result.set_read_result(recordList)
            query_result.set_is_successful(True)

            return query_result
        except Exception as e:
            return query_result

    """
    # Update a record with specified key and columns
    # Return Query Result upon successful Update
        # is_successful = True
        # key = key
        # column_data = (*columns): List
    # Return Query Result upon Failed Update or if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        # is_successful = False
        # key = key
        # column_data = "none"
    # LOCK occurs inside the table on the record and PageRange
    """
    def update(self, key, *columns):
        query_result = QueryResult()
        query_result.set_key(key)

        # rid is the rid of base record
        rids = self.table.index.locate(self.table.key, key)
        try:
            for rid in rids:
                prevRecordColData = self.table.updateRecord(self.table.key, rid, columns)

            query_result.set_column_data(prevRecordColData)
            query_result.set_is_successful(True)
            return query_result
        except Exception as e:
            return query_result

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    # LOCK occurs on the record from Query class
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        query_result = QueryResult()
        ridRange = self.table.index.locate_range(start_range, end_range, self.table.key)

        if ridRange == []:
            return query_result
        sum = 0
        for rid in ridRange:
            record = self.table.getLatestupdatedRecord(rid)
            value = record.columns[aggregate_column_index]
            sum += value

        query_result.set_is_successful(True)
        query_result.set_read_result(sum)
        return query_result

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    # Locking by increment should be handled by the select and update functions
    """
    def increment(self, key, column):
        query_result = QueryResult()
        query_result.set_key("none")

        r = self.select(key, self.table.key, [1] * self.table.num_all_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_all_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)

            return u
        return False
