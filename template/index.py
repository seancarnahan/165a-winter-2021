"""
A data structure holding indices for various columns of a table. Key column should be indexed by default,
other columns can be indexed through this object. Indices are usually B-Trees, but other data structures
can be used as well.

Index will be RHash
"""
from collections import defaultdict
from query import Query


class InvalidIndexError(Exception):
    def __init__(self, message):
        super().__init__(message)


class InvalidColumnError(Exception):
    def __init__(self, message):
        super().__init__(message)


class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] * table.num_columns
        self.seeds = [None] * table.num_columns
        self.table = table
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        if self.indices[column] is None:
            err = "Column {0} has no index".format(column)
            raise InvalidIndexError(err);

        return self.indices[column][value]

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            err = "Column {0} has no index".format(column)
            raise InvalidIndexError(err);
        pass

    def create_index(self, column_number):
        """
        Create an index for the specified column

        :param column_number: int       # position of colum
        """
        try:
            self.indices[column_number] = defaultdict(list)
        except IndexError:
            err = "Column " + str(column_number) + " does not exist"
            raise InvalidColumnError(err)

        query = Query()

        for record in query.select():
            if record.indirection is None:
                self.indices[column_number][record.columns[column_number]].append(record.rid);

        pass

    def drop_index(self, column_number):
        """
        Drops the index for the specified column

        :param column_number: int         # position of column
        """
        try:
            self.indices[column_number] = None
        except IndexError:
            err = "No index exists for column " + str(column_number)
            raise InvalidIndexError(err);


# Testing
if __name__ == "__main__":
    from table import Table

    tb = Table("TestTable", 5, 0)

    tb.index.create_index(tb.key)

    tb.index.locate(0, 5)

    tb.index.locate(1, 5)
