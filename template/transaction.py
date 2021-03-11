import copy
# import numpy as np

from collections import defaultdict
from template.query import Query


def list_diff(list1, list2):
    """
    Returns the difference between two lists.
    Used for checking original buffer_pool bp_tailRecordsSinceLastMerge object and changed version post-transaction.

    :param list1: list
    :param list2: list
    """
    tempList1 = set(tuple(x[:2]) for x in list1)
    tempList2 = set(tuple(x[:2]) for x in list2)
    return list(list(tempList1 - tempList2) + list(tempList2 - tempList1))


class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.rollback_queries = []
        self.db = None
        self.db_buffer_pool = None
        self.bp_recordsInPageRange = []  # list of lists from buffer pool
        self.acquiredReadLocks = defaultdict(set)  # key: table_name ; values = [RIDs]
        self.acquiredWriteLocks = defaultdict(set)  # key: table_name ; values = [RIDs]

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
        if self.queries:
            # get pre-transaction page ranges in buffer pool before querying
            front_query_obj = self.parse_query_method(self.queries[0][0])
            self.db = front_query_obj.table.parent_db
            self.db_buffer_pool = self.db.bufferPool
            self.bp_recordsInPageRange = copy.deepcopy(self.db_buffer_pool.recordsInPageRange)

        for query, args in self.queries:
            query_obj = self.parse_query_method(query)
            query_type = str(query).split()[2].split(".")[1]
            table_name = query_obj.table.table_name
            index = query_obj.table.index
            lock_manager = query_obj.table.lock_manager

            if query_type in ("update", "delete"):
                rids = index.locate(query_obj.table.key, args[0])

                for rid in rids:
                    if lock_manager.acquireWriteLock(rid) or rid in self.acquiredWriteLocks[table_name]:
                        self.acquiredWriteLocks[table_name].add(rid)
                    elif rid in self.acquiredReadLocks[table_name]:
                        if lock_manager.upgradeReadLock(rid):
                            self.acquiredReadLocks[table_name].remove(rid)
                            self.acquiredWriteLocks[table_name].add(rid)
                        else:
                            return self.abort()
                    else:
                        return self.abort()

            elif query_type in ("select", "sum"):
                if query_type == "select":
                    rids = index.locate(args[1], args[0])
                elif query_type == "sum":
                    rids = index.locate_range(args[0], args[1], query_obj.table.key)

                for rid in rids:
                    if rid in self.acquiredWriteLocks[table_name] or rid in self.acquiredReadLocks[table_name]:
                        continue
                    elif lock_manager.acquireReadLock(rid):
                        self.acquiredReadLocks[table_name].add(rid)
                    else:
                        return self.abort()

            result = query(*args)

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

        # Release read locks
        for table_name in self.acquiredReadLocks.keys():
            table = self.db.get_table(table_name)
            for rid in self.acquiredReadLocks[table_name]:
                table.lock_manager.releaseReadLock(rid)

        # Release write locks
        for table_name in self.acquiredWriteLocks.keys():
            table = self.db.get_table(table_name)
            for rid in self.acquiredWriteLocks[table_name]:
                table.lock_manager.releaseWriteLock(rid)

        return False

    """
    :param -> bound_method: bound method to a query object
    
    # returns the query object instance from the bound method
    # "helper method" 
    """
    def parse_query_method(self, bound_method) -> Query:
        return bound_method.__self__

    def affected_page_ranges(self, changed_tRSLM):  # changed_tailRecordsSinceLastMerge
        """
        Get a dict of the affected PageRanges in a table by comparing the original buffer pool data to the data after
        the transaction finishes.

        :param changed_tRSLM: list - list of lists containing [table_name, pr_index, count]
        :return: dict of { 'table_name': [pr_index1, pr_index2] }
        """
        dict_of_PRs_to_commit = {}  # dict of { 'table_name': [pr_index1, pr_index2] }

        # list of lists in changed_tRSLM not originally in bp_tailRecordsSinceLastMerge
        new_prs = list_diff(changed_tRSLM, self.bp_recordsInPageRange)

        for pr_list in changed_tRSLM:
            # create a new key with table_name
            if pr_list[0] not in dict_of_PRs_to_commit.keys():
                dict_of_PRs_to_commit[pr_list[0]] = []

            for old_pr_list in self.bp_recordsInPageRange:
                # iterate through all lists and find matching table_name, pr_index
                # we need to commit that pr if changes have been made to it
                if pr_list[0] == old_pr_list[0] and pr_list[1] == old_pr_list[1] and pr_list[2] != old_pr_list[2]:
                    dict_of_PRs_to_commit[pr_list[0]].append(pr_list[1])  # append the PR_index

        if new_prs:
            # note: new_prs is a list of lists having the table_name and pr_index
            # there is no record count
            for pr_list in new_prs:
                # create a new key with table_name
                if pr_list[0] not in dict_of_PRs_to_commit.keys():
                    dict_of_PRs_to_commit[pr_list[0]] = []
                dict_of_PRs_to_commit[pr_list[0]].append(pr_list[1])  # append the PR_index

        return dict_of_PRs_to_commit

    def commit(self):
        """
        transaction completes ->
            write to disk manually, reset dirty bit to 0
            for all the page ranges we wrote to disk
            then release all the locks

        lock_manager.unlock_records( # locked_records = [RIDs])
        :return: True, if commit is successful
        """
        changed_tRSLM = copy.deepcopy(self.db_buffer_pool.recordsInPageRange)
        dict_of_PRs_to_commit = self.affected_page_ranges(changed_tRSLM=changed_tRSLM)

        for table_name in dict_of_PRs_to_commit.keys():
            # only need to write to disk if there are any PageRanges to write
            if dict_of_PRs_to_commit[table_name]:
                for pr_index in dict_of_PRs_to_commit[table_name]:
                    # get the PageRange object from buffer_pool
                    pr_obj = self.db_buffer_pool.get_page_range_from_buffer_pool(table_name, pr_index)
                    if pr_obj is None:
                        self.db_buffer_pool.requestPageRange(table_name, pr_index)
                        pr_obj = self.db_buffer_pool.get_page_range_from_buffer_pool(table_name, pr_index)

                    self.db_buffer_pool.write_to_disk(pr_obj)

            # release list of RIDs with read locks, release list of RIDs with write locks
            self.db.get_table(table_name).lock_manager.releaseLocks(
                self.acquiredReadLocks[table_name],
                self.acquiredWriteLocks[table_name])

        return True
