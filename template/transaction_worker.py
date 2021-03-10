from template.table import Table, Record
from template.index import Index
import threading


class TransactionWorker:
    """
    # Creates a transaction worker object.
    """

    def __init__(self, transactions: list = None):
        if transactions is None:
            transactions = []
        self.stats = []
        self.transactions = transactions
        self.result = 0

        self.thread = threading.Thread(target=self.execute, args=())

    """
    Appends t to transactions
    """

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    Runs a transaction
    """

    def execute(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))

    def run(self):
        self.thread.start()

    def waitTilCompletion(self):
        self.thread.join()
