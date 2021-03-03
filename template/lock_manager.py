
class LockManager():
    """
    # Initialize lock manager
    #locks:
        0 -> unlocked
        1 -> exclusive lock
        2 -> shared lock
        (0-numOFThreads) = Key -> B
        (0-2) = lockStatus -> A
        AB
    #responsible for maintaining state of locks on each record
    """
    def __init__(self):
        locked_records = {RID: LOCK}
        pass

    """
    #return TRUE, if there able to perform the query, false otherwise
    
    """
    def get_record_lock_status(self, RID, queryType):
        # type -> INSERT, READ, WRITE
        pass

    def lock_record(self, rid):
        pass

    def unlock_records(self, records):
        pass

