from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = [] #Insert,Select,SUm
        self.rollbackQueries = []
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


    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            else:
                #create rollback for the successful query
                self.createRollBackQuery()

        return self.commit()

    def createRollBackQuery(self):
        #do research
        isinstance()
        pass

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        #run all the queries in rollBackQueries, in reverse order
        return False

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