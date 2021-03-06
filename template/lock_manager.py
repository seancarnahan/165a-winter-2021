from collections import defaultdict
from template.ReadWriteLock import ReadWriteLock


class LockManager:
    """
    Responsible for maintaining state of locks on each record
    """

    def __init__(self):
        self.record_locks = defaultdict(ReadWriteLock)

    def acquireTableLock(self) -> bool:
        return True

    def acquireTableLock(self) -> bool:
        return True

    def acquirePageRangeLock(self, page_range_index: int) -> bool:
        return True

    def releasePageRangeLock(self, page_range_index: int) -> bool:
        return True

    def acquireReadLock(self, rid: int) -> bool:
        """ Acquire a reading lock. Returns True if successful, False otherwise. """
        lock = self.record_locks[rid]
        lock.acquire_read()
        return True

    def releaseReadLock(self, rid: int) -> bool:
        """ Release a reading lock. Returns True if successful, False otherwise. """
        lock = self.record_locks[rid]
        lock.release_read()
        return True

    """NOT sure if we need the below methods"""
    def acquireWriteLock(self, rid: int) -> bool:
        """ Acquire a writing lock. Returns True if successful, False otherwise. """
        lock = self.record_locks[rid]
        lock.acquire_write()
        return True

    def releaseWriteLock(self, rid: int) -> bool:
        """ Release a writing lock. Returns True if successful, False otherwise. """
        pass

    def releaseLocks(self, rids: list) -> bool:
        """ Release locks associate with rids. """
        pass
