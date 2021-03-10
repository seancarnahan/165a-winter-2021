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

        self.thread = threading.Thread(target=self.execute, args=(self.transactions, self.stats, self.result))

    """
    Appends t to transactions
    """

    def add_transaction(self, t):
        self.transactions.append(t)

    @staticmethod
    def execute(transactions, stats, result):

        for transaction in transactions:
            # each transaction returns True if committed or False if aborted
            stats.append(transaction.run())
        # stores the number of transactions that committed
        result = len(list(filter(lambda x: x, stats)))

    """
    Runs a transaction
    """
    def run(self):
        self.thread.start()

