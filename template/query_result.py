
class QueryResult:
    """
    # Creates a QueryResult object.
    # passed between from query object to Transaction object to get the result
    """
    def __init__(self):
        self.is_successful = False
        self.key = None
        self.column_data = None

    def set_is_successful(self, is_successful: bool):
        self.is_successful = is_successful

    def set_key(self, key):
        self.key = key

    def set_column_data(self, column_data):
        self.column_data = column_data
