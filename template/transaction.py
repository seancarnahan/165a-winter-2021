

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.rollback_queries = []
        # locked_records = [RIDs]
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    """
    # runs queries in self.queries
    # aborts if the query result = false
    # should always returns True if commits and False if aborts
    """
    def run(self):
        for query, args in self.queries:
            query_type = str(query).split()[2].split(".")[1]
            result = query(*args)
            is_successful = None
            key = None
            column_data = None

            if query_type == "select" or query_type == "sum":
                # result is either the selection or the sum, or False if failed
                # does not create a rollback for sum or select
                is_successful = result
            else:
                # result is now QueryResult Object
                is_successful = result.is_successful
                key = result.key
                column_data = result.column_data

            # If the query has failed the transaction should abort
            if not is_successful:
                return self.abort()
            else:
                # create rollback for the successful query
                self.create_rollback(key, query, query_type, column_data)

        return self.commit()

    """
    :param -> key: indexed location of record
    :param -> boundQueryMethod: query object sent straight from tester
    :param(optional) -> columnData: returned from result of query (should be the prevRecord's column data)
    """
    def create_rollback(self, key, bound_query_method, query_type, column_data=None):
        query_object = self.parse_query_method(bound_query_method)

        if query_type == "insert":
            self.rollback_queries.append((self.rollback_insert, key, query_object, None))
        elif query_type == "select" or query_type == "sum":
            # nothing to rollback
            pass
        elif query_type == "update":
            self.rollback_queries.append((self.rollback_update, key, query_object, column_data))
        elif query_type == "delete":
            self.rollback_queries.append((self.rollback_delete, None, query_object, column_data))
        else:
            print("ERROR invalid query type")

    """
    :param -> key: indexed location of record
    :param -> query_object: query object sent from tester
    
    # rolls back an insert by deleting the record
    """
    def rollback_insert(self, key, query_object):
        # delete the inserted record
        query_object.delete(key)

    """
    :param -> key: indexed location of record
    :param -> query_object: query object sent from tester
    :param -> column_data: returned from result of query (should be the prevRecord's column data)
    
    # rolls back an update by adding another update with the values from the prev Tail Record
    # or the base record if there is no other tail record
    """
    def rollback_update(self, key, query_object, column_data):
        # updates the records with the previous values before update
        query_object.update(key, column_data)

    """
    :param -> key: indexed location of record
    :param -> query_object: query object sent from tester
    :param -> column_data: returned from result of query (should be the prevRecord's column data)
    
    # rolls back a delete by doing an insert with the values from the last tail record
    # or the base record if there is no other tail record
    """
    def rollback_delete(self, query_object, column_data):
        # insert another record with the same values as the deleted record
        query_object.insert(column_data)

    """
    # runs all the functions in self.rollbackQueries
    # tuple: (self.rollback_update, key(optional=None), query_object, column_data(optional=None))
    """
    def abort(self):
        for tup in self.rollback_queries:
            rollback_query = tup[0]
            key = tup[1]
            query_obj = tup[2]
            col_data = tup[3]

            # execute the rollback function
            if key is None:
                # rollback delete
                rollback_query(query_obj, col_data)
            elif col_data is None:
                # rollback insert
                rollback_query(key, query_obj)
            else:
                # rollback update
                rollback_query(key, query_obj, col_data)

        return False

    """
    :param -> bound_method: bound method to a query object
    
    # returns the query object instance from the bound method
    # "helper method" 
    """
    def parse_query_method(self, bound_method):
        return bound_method.__self__

    def commit(self):
        # TODO: commit to database
        """
        #transaction completes ->
            write to disk manually, reset dirty bit to 0
            for all the page ranges we wrote to disk
            then release all the locks

        lock_manager.unlcok_records( # locked_records = [RIDs])
        """
        return True