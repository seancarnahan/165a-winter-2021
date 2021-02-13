from template.config import *


class Page:
    """
    A Page Object stores the actual data.

    The number of records per Page is determined by PAGE_SIZE found in config.py

    Every entry into the page is 4 bytes.

    In total, a Page can store 4000 kb of data.
    """

    def __init__(self):
        self.num_records = 0 #max: 1000
        self.data = bytearray()

    def has_capacity(self):
        """ Return True if Page has capacity, otherwise return False """
        if self.num_records >= PAGE_SIZE:
            return False
        else:
            return True

    def write(self, value):
        """
        Write value to the page.

        :param value: int
        :returns: True if write is successful otherwise False
        """
        if self.has_capacity():
            byteArray = Page().integerToBytes(value)

            self.data += byteArray
            self.num_records += 1  

            return True #Successfully added record to page
        else:
            return False #Failed to add record to page 

    def getRecord(self, index):
        """
        Read a value from the page that corresponds to the given index of a record.

        :param index: int

        :returns: the value at the given index. Return False if read is unsuccessful.
        """
        x1, x2 = Page().getRecordIndexes(index)
    
        try:
            return Page().hexToInt(Page().bytesToHex(self.data[x1: x2]))
        except ValueError:
            return False

    def replaceRecord(self, index, newIntVal):
        """
        Replace the value at index with newIntVal.

        :param index: int
        :param newIntVal: int

        :returns: True if replace is successful. Otherwise False.
        """
        if index > self.num_records:
            return False

        x1, x2 = Page().getRecordIndexes(index)
        byteArray = Page().integerToBytes(newIntVal)
        self.data[x1: x2] = byteArray
        return True

    @staticmethod
    def getRecordIndexes(index):
        """
        Remap index to corresponding index but in increments of PAGE_RECORD_SIZE.

        :param index: int
        :returns: a list of two values. [StartOfRecord Index (inclusive), EndOfRecord Index (exclusive)]
        """
        return [index * PAGE_RECORD_SIZE, (index+1) * PAGE_RECORD_SIZE]

    @staticmethod
    def integerToBytes(integer):
        """
        Convert an integer to 4 bytes

        :param integer: int
        :returns: bytearray containing bytes of integer.
        """

        # Hexed version of integer must be 8 bit?
        return (integer).to_bytes(4,'big')

    @staticmethod
    def bytesToHex(bytesArray):
        """ Converts a bytearray into hexadecimal values"""
        return bytesArray.hex()

    @staticmethod
    def hexToInt(hexStr):
        """ Converts a string containing hex values into an integer """
        return int(hexStr, 16)
