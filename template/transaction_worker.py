from template.table import Table, Record
from template.index import Index
import threading
import random

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

    """
    Processes transactions
    """
    @staticmethod
    def execute(transactions, stats, result):
        print("------Execution Thread------- ")
        id = random.random()
        print("Num of Transactions: " + str(len(transactions)))
        print("Creating ID: " + str(id))
        counter = 0

        for transaction in transactions:
            # each transaction returns True if committed or False if aborted
            stats.append(transaction.run())

            print("id: " + str(id) + "; transaction: " + str(counter))
            counter += 1

            # stores the number of transactions that committed
        result = len(list(filter(lambda x: x, stats)))

    """
    Runs a transaction
    """

    def run(self):
        # print("Num of Transactions: " + str(len(self.transactions)))
        self.thread.start()
