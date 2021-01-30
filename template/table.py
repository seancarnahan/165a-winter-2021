from template.page import Page
from template.index import Index
from template.config import *

from time import time

class Record:

    #input: columns = list of integers
    def __init__(self, key, indirection, RID, timestamp, encoding, columns):
        #not sure
        self.key = key

        #an RID if this is an update
        #set to NULL or NONE if this is the BASE record
        self.indirection = indirection

        #the record identifier
        self.RID = RID

        #integer (based on milliseconds from epoch)
        self.timestamp = timestamp

        #bitmap
        self.encoding = encoding

        #list corresponding to all of the user created columns
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns + RECORD_COLUMN_OFFSET
        self.page_directory = PageDiretory(self.num_columns)
        self.index = Index(self)

        self.latestRID = None
        self.currPageRangeIndex = 0
        self.pageRanges = self.generatePageRangeMapping()

    #INSERT -> base pages
    #columns: list of integers with their index mapped to their corresponding column
    def createNewRecord(self, key, columns):
        indirection = None
        RID = self.getNewRID()
        timeStamp = self.getNewTimeStamp()
        encoding = self.getInitialEncoding()

        #create Record Object
        record = Record(key, indirection, RID, timeStamp, encoding, columns)

        #create new Page Range
        if self.latestRID > (PAGE_SIZE * (self.currPageRangeIndex + 1)) - 1:
            newPageRange = PageRange(self.num_columns)

            self.pageRanges.append(newPageRange)
            self.currPageRangeIndex += 1


        #add record to Page Range based on currPageRangeIndex
        pageRange = self.pageRanges[self.currPageRangeIndex]
        pageRange.setBasePageRecord(record)

    #UPDATE -> tail pages
    def updateRecord(self):
        pass

    #DELETE
    def deleteRecord(self):
        pass

    #SELECT
    def getRecord(self):
        pass

    #ranged queries
    def getRecords(self):
        pass

    #list of pageRanges
    #index (0 = within 1000 records, 1 = 1001 - 2000 records) based on config.pageSize
    def generatePageRangeMapping(self):
        return [PageRange(self.num_columns)]

    def getNewRID(self):
        if self.latestRID == None:
            self.latestRID = 0
        else:
            self.latestRID += 1

        return self.latestRID

    def getInitialEncoding(self):
        #TODO: maybe swithch this to a bitmap
        encoding = ""
        for i in range(self.num_columns):
            encoding += "0"
        return encoding

    def getNewTimeStamp(self):
        #miliseconds from epoch as an integer
        return round(time().time() * 1000)

    def updateIndirection(self):
        pass

    #Ignore merge until milestone 2
    # def __merge(self):
    #     pass

class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.basePages = []

        #key: column index
        #value: list of tailpages, when one tail page runs out add a new on to the list
        self.tailPages = {}

        for col in range(num_columns):
            self.basePages.append(Page())

            self.tailPages[col] = [Page()]

    def setBasePageRecord(self, record):
        self.basePages[INDIRECTION_COLUMN].write(record.indirection)
        self.basePages[RID_COLUMN].write(record.RID)
        self.basePages[TIMESTAMP_COLUMN].write(record.timestamp)
        self.basePages[SCHEMA_ENCODING_COLUMN].write(record.encoding)

        for col in range(RECORD_COLUMN_OFFSET, RECORD_COLUMN_OFFSET + len(record.columns)):
            columnData = record.columns[col - RECORD_COLUMN_OFFSET]

            self.basePages[col].write(columnData)

# Given a RID, the page directory returns the location of the certain
# record inside the page within the page range. The efficiency of this data structure is a
# key factor in performance.
class PageDiretory:

    def __init__(self, num_columns):
        pass



