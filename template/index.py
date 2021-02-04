"""
A data structure holding indices for various columns of a table. Key column should be indexed by default,
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures
can be used as well.

Index will be RHash
"""
from collections import defaultdict
from template.config import *



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

    def insert(self, rid, values):
        for i in len(self.indices):
            if self.indices[i] is not None:
                self.insertIntoIndex(i, rid, values[i])

    def updateIndexes(self, rid, oldValues, newValues):
        for i in len(self.indices):
            if self.indices[i] is not None:
                if oldValues[i] != newValues[i]:
                    self.update_index(i, rid, oldValues[i], newValues[i])

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
        index = self.indices[column]

        currValue = self.seeds[column][0]

        if begin > self.seeds[column][1]:
            currValue = self.seeds[column][1]

        if begin > self.seeds[column][2]:
            currValue = self.seeds[column][2]

        while currValue < end:
            matching_rids.extend(index[currValue][0])
            currValue = index[currValue][1]

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

        index = self.indices[column_number]

        for pageRange in self.table.page_directory.pageRanges:
            for basePage in pageRange.basePages:
                for i in range(basePage.numOfRecords):
                    value = basePage.physicalPages[column_number].getRecord(i)
                    rid = basePage.physicalPages[RID_COLUMN].getRecord(i)
                    index[value].append(rid)

        sortedKeys = list(index)

        for key1, key2 in zip(sortedKeys[:-1], sortedKeys[1:]):
            index[key1] = [index[key1], key2]

        if len(sortedKeys) != 0:
            index[sortedKeys[-1]] = [index[sortedKeys[-1]], None]
            self.createSeeds(column_number)

        return

    def drop_index(self, column_number):
        """
        Drops the index for the specified column

        :param column_number: int         # position of column
        """
        try:
            self.indices[column_number] = None
            self.seeds[column_number] = None
        except IndexError:
            raise InvalidIndexError(column_number)

    def update_index(self, column_number, rid, oldValue, newValue):
        """
        Update index for the specified column

        :param column_number: int
        :param rid: rid you are updating
        :param oldValue: int. The old value of the record at the specified column
        :param newValue: int. The new value of the record at the specified column
        """
        try:
            if self.indices[column_number] is None:
                raise InvalidIndexError(column_number)
        except IndexError:
            raise InvalidColumnError(column_number)

        index = self.indices[column_number]

        # Remove the RID from the index
        index[oldValue].remove(rid)

        # If the new value hasn't been indexed by the column index then we want to add it in and insert it
        # at the correct spot in our linked list.
        if newValue not in index.keys():
            self.createNewKeyEntry(index, self.seeds[column_number], newValue)

        index[newValue][0].append(rid)

        self.updateMedianSeed(self.seeds[column_number], index.keys())

        return True

    def insertIntoIndex(self, column_number, rid, value):
        """
        Insert into the index for the specified column.

        :param column_number: int
        :param rid: int. New records that are to be inserted into the index.
        :param value: int. Values for the specified column of the record
        """
        try:
            if self.indices[column_number] is None:
                raise InvalidIndexError(column_number)
        except IndexError:
            raise InvalidColumnError(column_number)

        index = self.indices[column_number]

        if value not in index.keys():
            self.createNewKeyEntry(index, self.seeds[column_number], value)

        index[value][0].append(rid)

        self.updateMedianSeed(list(index.keys()))

        return True

    def createSeeds(self, column_number):
        keys = list(self.indices[column_number].keys())
        keys.sort()
        minKey = keys[0]
        maxKey = keys[-1]
        medianKey = keys[int(len(keys) / 2)]

        self.seeds[column_number] = [minKey, medianKey, maxKey]

    def createNewKeyEntry(self, index, seeds, key):

        if key < seeds[0]:
            index[key] = [[], seeds[0]]
            self.updateMinSeed(seeds, key)
            return
        elif key > seeds[2]:
            index[key] = [[], None]
            index[seeds[2]][1] = key
            self.updateMaxSeed(seeds, key)
            return

        if key < seeds[1]:
            prevKey = seeds[0]
        else:
            prevKey = seeds[1]

        while prevKey < key:
            prevKey = index[prevKey][1]  # find largest key that is < newKey

        nextKey = index[prevKey][1]  # store the key that the newKey should point to
        index[prevKey][1] = key  # make the prevKey point to the new key
        index[key] = [[], nextKey]  # make the new key point to the smallest key larger than newKey

    def updateMaxSeed(self, seeds, value):
        seeds[2] = value

    def updateMinSeed(self, seeds, value):
        seeds[0] = value

    def updateMedianSeed(self, seeds, values):
        seeds[1] = values[int(len(values) / 2)]