from collections import defaultdict
from template.RWLock import RWLock
from threading import Lock


class LockManager:
    """
    Responsible for maintaining state of locks on each record
    """

    def __init__(self):
        self.managerLock = Lock()
        self.record_locks = defaultdict(RWLock)
        self.table_lock = Lock()
        self.page_range_locks = defaultdict(Lock)

    def acquireTableLock(self) -> bool:
        return self.table_lock.acquire()

    def releaseTableLock(self):
        self.table_lock.release()

    def acquirePageRangeLock(self, page_range_index: int) -> bool:
        with self.managerLock:
            lock = self.page_range_locks[page_range_index]
        return lock.acquire()

    def releasePageRangeLock(self, page_range_index: int) -> bool:
        with self.managerLock:
            lock = self.page_range_locks[page_range_index]
        lock.release()
        return True

    def acquireReadLock(self, rid: int) -> bool:
        """ Acquire a reading lock. Returns True if successful, False otherwise. """
        with self.managerLock:
            lock = self.record_locks[rid]
        return lock.r_acquire()

    def releaseReadLock(self, rid: int) -> bool:
        """ Release a reading lock. Returns True if successful, False otherwise. """
        with self.managerLock:
            lock = self.record_locks[rid]
        return lock.r_release()

    def acquireWriteLock(self, rid: int) -> bool:
        """ Acquire a writing lock. Returns True if successful, False otherwise. """
        with self.managerLock:
            lock = self.record_locks[rid]
        return lock.w_acquire()

    def releaseWriteLock(self, rid: int) -> bool:
        """ Release a writing lock. Returns True if successful, False otherwise. """
        with self.managerLock:
            lock = self.record_locks[rid]
        return lock.w_release()

    def upgradeReadLock(self, rid: int) -> bool:
        with self.managerLock:
            lock = self.record_locks[rid]
        return lock.upgradeLock()

    def downgradeWriteLock(self, rid: int) -> bool:
        with self.managerLock:
            lock = self.record_locks[rid]
        return lock.downgradeLock()

    def releaseLocks(self, readRids: list, writeRids: list) -> bool:
        """ Release locks associate with rids. """
        readLocks = []
        writeLocks = []
        with self.managerLock:
            for rid in readRids:
                readLocks.append(self.record_locks[rid])
            for rid in writeRids:
                writeLocks.append(self.record_locks[rid])

        for lock in readLocks:
            lock.r_release()

        for lock in writeLocks:
            lock.w_release()