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
        self.page_directory = PageDiretory(num_columns)
        self.index = Index(self)

        self.latestRID = None
        self.currPageRangeIndex = 0


    #INSERT -> base pages
    #columns: list of integers with their index mapped to their corresponding column
    #test that this works
    def createNewRecord(self, key, columns):

        indirection = None
        RID = self.getNewRID()
        timeStamp = self.getNewTimeStamp()
        encoding = 0

        #create Record Object
        record = Record(key, indirection, RID, timeStamp, encoding, columns)

        #create new Page Range
        if self.latestRID > (PAGE_SIZE * (self.currPageRangeIndex + 1)) - 1:
            newPageRange = PageRange(self.num_columns)

            self.page_directory.pageRanges.append(newPageRange)
            self.currPageRangeIndex += 1

        #add record to Page Range based on currPageRangeIndex
        pageRange = self.page_directory.pageRanges[self.currPageRangeIndex]
        pageRange.setBasePageRecord(record)


    #UPDATE -> tail pages
    #RID: is the record that you would like to update
    #values: is the values that you would like to add, excluding encoding, timestamp etc
    def updateRecord(self, key, RID, values):

        #record location -> could be a tail or base
        currPageRangeIndex = self.page_directory.getRecordLocation(RID, True)[0]
        currPageIndex = self.page_directory.getRecordLocation(RID, True)[1]
        currTailPageIndex = self.page_directory.getRecordLocation(RID, True)[2]

        currEncoding = self.page_directory.pageRanges[currPageRangeIndex].basePages[SCHEMA_ENCODING_COLUMN].getRecord(currPageIndex)
        currIndirection = self.page_directory.pageRanges[currPageRangeIndex].basePages[INDIRECTION_COLUMN].getRecord(currPageIndex)
        
        #check the encoding of the base page until you find the latest updated record -> could be tail record or base record
        while(currEncoding != 1):
            #check for record in tail pages
            currPageRangeIndex = self.page_directory.getRecordLocation(currIndirection, False)[0]
            currPageIndex = self.page_directory.getRecordLocation(currIndirection, False)[1]
            currTailPageIndex = self.page_directory.getRecordLocation(currIndirection, False)[2]

            currEncoding = self.page_directory.pageRanges[currPageRangeIndex].tailPages[SCHEMA_ENCODING_COLUMN][currTailPageIndex].getRecord(currPageIndex)
            currIndirection = self.page_directory.pageRanges[currPageRangeIndex].tailPages[INDIRECTION_COLUMN][currTailPageIndex].getRecord(currPageIndex)

        #currIndirection should be set to None
        #currEncoding should be set to None
        #new tail page RID
        newRID = self.getNewRID()
        pageRange = self.page_directory.pageRanges[currPageRangeIndex]

        #set the latest updated record indirection to new tail page RID
        pageRange[INDIRECTION_COLUMN] = newRID

        #set the latest updated record encoding to 1
        pageRange[SCHEMA_ENCODING_COLUMN] = 1


        #create tail page record
        indirection = None
        timeStamp = self.getNewTimeStamp()
        encoding = 0

        tailRecord = Record(key, indirection, newRID, timeStamp, encoding, values)

        #check if tail page has exceeded its limits: if true then create new page range

        #add tail page record to tail page

    def getNewRID(self):
        if self.latestRID == None:
            self.latestRID = 0
        else:
            self.latestRID += 1

        return self.latestRID

    #test: make sure this works
    def getNewTimeStamp(self):
        #miliseconds from epoch as an integer
        return round(time().time() * 1000)


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

    def setTailPageRecord(self, record):
        self.basePages[INDIRECTION_COLUMN].write(record.indirection)
        self.basePages[RID_COLUMN].write(record.RID)
        self.basePages[TIMESTAMP_COLUMN].write(record.timestamp)
        self.basePages[SCHEMA_ENCODING_COLUMN].write(record.encoding)


#PageDirectory = [PageRange()]
class PageDiretory:

    def __init__(self, num_columns):
            #list of pageRanges
            #index (0 = within 1000 records, 1 = 1001 - 2000 records) based on config.pageSize
            self.pageRanges = [ PageRange(num_columns) ]

    #input: RID; True = base type; False = tail type
    #base page output: [pageRangeIndex, index location of record with the Page Range]
    #tail page output: [pageRangeIndex, index location of record with the Page Range, tailPageIndex]
    def getRecordLocation(self, RID, head_flag):
        pass




