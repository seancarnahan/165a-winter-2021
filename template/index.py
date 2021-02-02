"""
A data structure holding indices for various columns of a table. Key column should be indexed by default,
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures
can be used as well.

Index will be RHash
"""
from collections import defaultdict, Iterable
from config import *


class InvalidIndexError(Exception):
    def __init__(self, column):
        super().__init__("Column {0} has no index".format(column))


class InvalidColumnError(Exception):
    def __init__(self, column):
        super().__init__("Column {0} does not exist".format(column))


class Index:
    """
    An Index for a column is a dictionary of lists.

    Ex: self.indices[column][columnValue] => [list of RIDS with columnValue]
    """

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.seeds = [None] * table.num_columns
        self.table = table

    def locate(self, column, value):
        """
        returns all records with the given value on column "column"

        :param column: int. Represents which column index to use. Must have a index created for it previously
        :param value: int. The value to search for
        :returns: A list of RIDs that have the value "value" in the column "column".
        """

        try:
            if self.indices[column] is None:
                raise InvalidIndexError(column)
        except IndexError:
            raise InvalidColumnError(column)

        return self.indices[column][value]

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end".

        :param begin: int. The start of the range.
        :param end: int. The end of the range.
        :param column: int. The column to search under with column's index.
        :returns: A list of RIDs that have a value that falls between [begin,end] in column "column".
        """
        try:
            if self.indices[column] is None:
                raise InvalidIndexError(column)
        except IndexError:
            raise InvalidColumnError(column)

        matching_rids = []
        for value in range(begin, end):
            if isinstance(self.indices[column][value], Iterable):
                matching_rids.extend(self.indices[column][value])

        return matching_rids

    def create_index(self, column_number):
        """
        Create an index for the specified column

        :param column_number: int
        """
        try:
            self.indices[column_number] = defaultdict(list)
        except IndexError:
            raise InvalidColumnError(column_number)

        for pageRange in self.table.page_directory.pageRanges:
            for basePage in pageRange.basePages:
                for i in range(basePage.numOfRecords):
                    value = basePage.physicalPages[column_number].getRecord(i)
                    rid = basePage.physicalPages[RID_COLUMN].getRecord(i)
                    self.indices[column_number][value].append(rid)

        return

    def drop_index(self, column_number):
        """
        Drops the index for the specified column

        :param column_number: int         # position of column
        """
        try:
            self.indices[column_number] = None
        except IndexError:
            raise InvalidIndexError(column_number)

    def update_index(self, column_number, rids, oldValues, newValues):
        """
        Update index for the specified column

        :param column_number: int
        :param rids: list of RIDs
        :param oldValues: list of ints. The ith entry of this list corresponds to the stored value already in
                         the table for the ith RID (in rids).
        :param newValues: list of ints. The ith entry of this list corresponds to the updated value that is to be indexed
                         for the ith RID (in rids).
        """
        try:
            if self.indices[column_number] is None:
                raise InvalidIndexError(column_number)
        except IndexError:
            raise InvalidColumnError(column_number)

        # Iterate over the updated RIDs and remove them from the index re-add them in.
        for rid, oldKey, newKey in zip(rids, oldValues, newValues):
            self.indices[column_number][oldKey].remove(rid)
            self.indices[column_number][newKey].append(rid)

        return True

    def insertIntoIndex(self, column_number, rids, values):
        """
        Insert into the index for the specified column.

        :param column_number: int
        :param rids: list of RIDs. New records that are to be inserted into the index.
        :param values: list of int. Values for the specified column of the ith record (in rids).
        """
        try:
            if self.indices[column_number] is None:
                raise InvalidIndexError(column_number)
        except IndexError:
            raise InvalidColumnError(column_number)

        for rid, key in zip(rids, values):
            self.indices[column_number][key].append(rid)

        return True
