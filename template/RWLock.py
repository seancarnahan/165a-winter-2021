from threading import Lock


class RWLock(object):
    """ RWLock class; this is meant to allow an object to be read from by
        multiple threads, but only written to by a single thread at a time.

        Original Source Code: https://gist.github.com/tylerneylon/a7ff6017b7a1f9a506cf75aa23eacfd6
        Credit: Tyler Neylon
    """

    def __init__(self):

        self.w_lock = Lock()
        self.num_r_lock = Lock()
        self.num_r = 0

    # ___________________________________________________________________
    # Reading methods.

    def r_acquire(self):
        if not self.num_r_lock.acquire(blocking=False):
            return False
        self.num_r += 1
        if self.num_r == 1:
            # If we can't acquire the write lock, there is a writer and no readers allowed.
            if not self.w_lock.acquire(blocking=False):
                self.num_r -= 1
                self.num_r_lock.release()
                return False
        self.num_r_lock.release()
        return True

    def r_release(self):
        assert self.num_r > 0
        self.num_r_lock.acquire()
        self.num_r -= 1
        if self.num_r == 0:
            self.w_lock.release()
        self.num_r_lock.release()

    # ___________________________________________________________________
    # Writing methods.

    def w_acquire(self):
        return self.w_lock.acquire(blocking=False)

    def w_release(self):
        self.w_lock.release()

    def upgradeLock(self):  # Read Lock -> Write Lock
        upgraded = False
        if self.num_r_lock.acquire(blocking=False):
            if self.num_r == 1:  # Write Lock is always acquired when there is at least one reader.
                self.num_r -= 1  # To "upgrade", you must be the only reader
                upgraded = True
            self.num_r_lock.release()
        return upgraded

    def downgradeLock(self): # Write Lock -> Read Lock
        downgraded = False
        if self.num_r_lock.acquire(blocking=False):
            if self.num_r <= 1:
                self.num_r += 1
                self.w_release()
                self.num_r_lock.release()
                downgraded = True
        return downgraded
