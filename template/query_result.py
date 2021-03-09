from collections import defaultdict

class QueryResult:
    """
    # Creates a QueryResult object.
    # passed between from query object to Transaction object to get the result
    """
    def __init__(self):
        self.is_successful = False
        self.key = None
        self.column_data = None
        self.read_locks = defaultdict(list)  # key: table_name ; values = [RIDs]
        self.write_locks = defaultdict(list)  # key: table_name ; values = [RIDs]
        self.read_result = None  # only set for SELECTs and SUMs

    def set_is_successful(self, is_successful: bool):
        self.is_successful = is_successful

    def set_key(self, key):
        self.key = key

    def set_column_data(self, column_data):
        self.column_data = column_data

    def set_write_lock(self, rid, table_name):
        self.write_locks[table_name].append(rid)

    def set_read_lock(self, rid, table_name):
        self.read_locks[table_name].append(rid)

    def set_read_result(self, read_result):
        self.read_result = read_result
