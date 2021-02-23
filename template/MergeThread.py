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
        Starts merge thread when called instead of when initialized
        """
        self.thread.start()  # Start the execution

    def run(self):
        """ Method that runs forever """

        self.exit_request.clear()

        while not self.exit_request.isSet():
            time.sleep(self.interval)
            print('\nInitiating a merge...')
            self.db.merge_placeholder()  # placeholder for db merge method

        """
        trigger merge based on either a) time or b) unmergedTailRecords threshold
        sort buffer_pool.merge_queue by number of unmerged tail records
        grab head from queue
        head has table_name and PR to merge on
        merge all tail pages in that PR
        reset number of unmerged tail records belonging to that table_name.PR to 0
        """

    def stop_thread(self):
        """
        sets the exit_request event flag to True so thread will terminate asap
        """
        self.exit_request.set()  # set event flag to True
