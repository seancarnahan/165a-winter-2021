import threading
import time


class MergeThread(threading.Thread):
    """
    Threading example class edited from:
    http://sebastiandahlgren.se/2014/06/27/running-a-method-as-a-background-thread-in-python/

    The run() method will be started and it will run in the background
    until the application exits OR the db is closed.
    """

    def __init__(self, db, interval=1):
        """
        MergeThread Constructor

        :type db: Database
        :param db: The Database to run the thread in
        :type interval: int
        :param interval: Check interval, in seconds. Defaults to 1
        """
        super().__init__()

        self.db = db
        self.interval = interval

        self.exit_request = threading.Event()

        self.thread = threading.Thread(target=self.run, args=())
        self.thread.daemon = True  # Daemonize thread

    def thread_start(self):
        """
        Starts merge thread when called instead of when initialized. Call when opening the Database.
        """
        self.thread.start()  # Start the execution

    def run(self):
        """ Method that begins waiting and merging within the Database """

        self.exit_request.clear()

        while not self.exit_request.isSet():

            print('\nInitiating a merge...')
            table_name, page_range_index = self.db.bufferPool.getPageRangeForMerge()  # get the info to start a merge
            if table_name == "empty":  # change this statement
                # there are no PageRanges to do a merge, ignore and wait
                pass
            else:
                # have PR to merge, start merge
                self.db.merge(table_name, page_range_index)

            time.sleep(self.interval)

    def stop_thread(self):
        """
        Sets the exit_request event flag to True so thread will terminate asap. Call when closing the Database.
        """
        self.exit_request.set()  # set event flag to True
