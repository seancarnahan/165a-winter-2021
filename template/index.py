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
        self.indices = [None] * table.num_all_columns
        self.seeds = [None] * table.num_all_columns
        self.table = table

    def insert(self, rid, values):
        """
        :param rid: int. rid to insert.
        :param values: list. values corresponding to rid.
        """
        for i in range(len(self.indices)):
            if self.indices[i] is not None:
                self.insertIntoIndex(i, rid, values[i-RECORD_COLUMN_OFFSET])

    def updateIndexes(self, rid, oldValues, newValues):
        """
        :param rid: int. rid to update.
        :param oldValues: list. old values corresponds to rid.
        :param newValues: list. updated values corresponding to rid.
        """
        for i in range(len(self.indices)):
            if self.indices[i] is not None:
                if oldValues[i-RECORD_COLUMN_OFFSET] != newValues[i-RECORD_COLUMN_OFFSET]:
                    self.update_index(i, rid, oldValues[i-RECORD_COLUMN_OFFSET], newValues[i-RECORD_COLUMN_OFFSETCLEAR])

    def remove(self, rid, values):
        """
        :param rid: int. rid to remove
        :param values: list. values corresponds to rid.
        """
        for i in range(len(self.indices)):
            if self.indices[i] is not None:
                self.removeFromIndex(i, rid, values[i])

    def locate(self, column, value):
        """
        returns all records with the given value on column "column"

        :param column: int. Represents which column index to use. Must have a index created for it previously
        :param value: int. The value to search for
        :returns: A list of RIDs that have the value "value" in the column "column".
        """

        try:
            if self.indices[column+RECORD_COLUMN_OFFSET] is None:
                raise InvalidIndexError(column+RECORD_COLUMN_OFFSET)
        except IndexError:
            raise InvalidColumnError(column+RECORD_COLUMN_OFFSET)

        if value not in self.indices[column+RECORD_COLUMN_OFFSET].keys():
            return []

        return self.indices[column+RECORD_COLUMN_OFFSET][value][0]

    def locate_range(self, begin, end, column):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end".

        :param begin: int. The start of the range.
        :param end: int. The end of the range.
        :param column: int. The column to search under with column's index.
        :returns: A list of RIDs that have a value that falls between [begin,end] in column "column".
        """
        try:
            if self.indices[column+RECORD_COLUMN_OFFSET] is None:
                raise InvalidIndexError(column)
        except IndexError:
            raise InvalidColumnError(column)

        matching_rids = []

        if end < self.seeds[column+RECORD_COLUMN_OFFSET][0] or begin > self.seeds[column+RECORD_COLUMN_OFFSET][2]:
            return matching_rids

        index = self.indices[column+RECORD_COLUMN_OFFSET]

        if self.seeds[column + RECORD_COLUMN_OFFSET][0] > begin:
            currKey = self.seeds[column+RECORD_COLUMN_OFFSET][0]  # set currKey to minKey
        else:
            currKey = begin

        if begin > self.seeds[column+RECORD_COLUMN_OFFSET][1]: # if begin > medianKey, skip everything before medianKey
            currKey = self.seeds[column+RECORD_COLUMN_OFFSET][1]

        if begin == self.seeds[column+RECORD_COLUMN_OFFSET][2]: # if begin = maxKey, make currKey maxKey
            currKey = self.seeds[column+RECORD_COLUMN_OFFSET][2]

        while currKey <= end:
            matching_rids.extend(index[currKey][0])
            currKey = index[currKey][1]
            if currKey is None:
                break

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

        return True

    def drop_index(self, column_number):
        """
        Drops the index for the specified column

        :param column_number: int         # position of column
        """
        try:
            self.indices[column_number] = None
            self.seeds[column_number] = None
            return True
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

        self.removeFromIndex(column_number, rid, oldValue)
        self.insertIntoIndex(column_number, rid, newValue)

        return True

    def removeFromIndex(self, column_number, rid, value):
        try:
            if self.indices[column_number] is None:
                raise InvalidIndexError(column_number)
        except IndexError:
            raise InvalidColumnError(column_number)

        index = self.indices[column_number]

        if value in index.keys():
            index[value][0].remove(rid)

            if len(index[value][0]) == 0:
                del index[value]

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
            self.createNewKeyEntry(index, column_number, value)

        index[value][0].append(rid)

        self.updateMedianSeed(column_number, list(index.keys()))

        return True

    def createSeeds(self, column_number):
        keys = list(self.indices[column_number].keys())
        keys.sort()
        minKey = keys[0]
        maxKey = keys[-1]
        medianKey = keys[int(len(keys) / 2)]

        self.seeds[column_number] = [minKey, medianKey, maxKey]

    def createNewKeyEntry(self, index, column, key):

        index[key] = [[], None]

        if self.seeds[column] is None or len(index.keys()) == 1:
            # there are no seeds generated so this is the first key in index. Make all seeds into this value.
            self.seeds[column] = [key,key,key]

        seeds = self.seeds[column]

        if key < seeds[0]:
            index[key] = [[], seeds[0]]
            self.updateMinSeed(column, key)
            return
        elif key > seeds[2]:
            index[key] = [[], None]
            index[seeds[2]][1] = key
            self.updateMaxSeed(column, key)
            return

        if key < seeds[1]:
            prevKey = seeds[0]
        else:
            prevKey = seeds[1]

        while index[prevKey][1] is not None:
            if index[prevKey][1] < key:
                prevKey = index[prevKey][1]  # find largest key that is < newKey
            else:
                break

        nextKey = index[prevKey][1]  # store the key that the newKey should point to
        index[prevKey][1] = key  # make the prevKey point to the new key
        index[key] = [[], nextKey]  # make the new key point to the smallest key larger than newKey

    def updateMaxSeed(self, column, value):
        self.seeds[column][2] = value

    def updateMinSeed(self, column, value):
        self.seeds[column][0] = value

    def updateMedianSeed(self, column, values):
        self.seeds[column][1] = values[int(len(values) / 2)]