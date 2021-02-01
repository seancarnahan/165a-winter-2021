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
        #self.currPageIndex = 0

    #INSERT -> base pages
    #columns: list of integers with their index mapped to their corresponding column
    #test that this works
    def createNewRecord(self, key, columns):

        RID = self.getNewRID()
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0

        #create Record Object
        record = Record(key, indirection, RID, timeStamp, encoding, columns)

        #insert a new record -> all the checks for capacity are done implicitly
        self.page_directory.insertBaseRecord(record)


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
        return int(time())

    #Ignore merge until milestone 2
    # def __merge(self):
    #     pass
class BasePage:
    def __init__(self, num_columns):
        self.basePage = []
        self.numOfRecords = 0

        for _ in range(num_columns):
            self.basePage.append(Page())


    def setBasePageRecord(self, record):
        self.basePage[INDIRECTION_COLUMN].write(record.indirection)
        self.basePage[RID_COLUMN].write(record.RID)
        self.basePage[TIMESTAMP_COLUMN].write(record.timestamp)
        self.basePage[SCHEMA_ENCODING_COLUMN].write(record.encoding)

        for col in range(RECORD_COLUMN_OFFSET, RECORD_COLUMN_OFFSET + len(record.columns)):
            columnData = record.columns[col - RECORD_COLUMN_OFFSET]

            self.basePage[col].write(columnData)
            self.numOfRecords += 1

    def hasCapacity(self):
        if self.numOfRecords >= PAGE_SIZE:
            #page is full
            return False
        else:
            #there is still room to add at least 1 more record
            return True
        

class TailPage:
    def __init__(self, num_columns):
        self.numOfRecords = 0
        pass

    def setTailPageRecord(self, record):
        # self.basePages[INDIRECTION_COLUMN].write(record.indirection)
        # self.basePages[RID_COLUMN].write(record.RID)
        # self.basePages[TIMESTAMP_COLUMN].write(record.timestamp)
        # self.basePages[SCHEMA_ENCODING_COLUMN].write(record.encoding)
        pass
    
    def hasCapacity(self):
        pass

class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.maxNumOfBasePages = self.getPageRangeCapacity()
        self.currBasePageIndex = 0
        
        #list of type BasePage
        self.basePages = [BasePage(self.num_columns)]

        #key: column index
        #value: list of type TailPage, when one tail page runs out add a new on to the list
        self.tailPages = {0: [TailPage(self.num_columns)]}

    def insertBaseRecord(self, record):
        currBasePage = self.basePages[self.currBasePageIndex]

        if currBasePage.hasCapacity():
            currBasePage.setBasePageRecord(record)
            return True #succesfully inserted a new record into Page Rage
        else:
            if self.addNewBasePage():
                self.currBasePageIndex += 1
                return True #succesfully inserted a new record into Page Rage
            else:
                #there is no more room in Page Range, need to tell PageDir to make a new one
                return False #FAILED to insert a record into page Range

    def insertTailRecord(self):
        pass

    def addNewBasePage(self):
        if self.hasCapacity():
            self.basePages.append(BasePage(self.num_columns))
            return True
        else:
            return False
  

    def addNewTailPage(self):
        pass

    #figure out how many many base pages can fit into PAGE RANGE without exceeding MAX_PAGE_RANGE_SIZE
    #return number of Base Pages
    def getPageRangeCapacity(self):
        maxBasePageSize = self.num_columns

        while((maxBasePageSize + self.num_columns) <=  MAX_PAGE_RANGE_SIZE):
            maxBasePageSize += self.num_columns

        return maxBasePageSize / self.num_columns

    #True if Page Range has capacity for another Base Page, if not PageDir should create another PageRange
    def hasCapacity(self):
        if len(self.basePages) >= self.maxNumOfBasePages:
            return False
        else:
            return True


#PageDirectory = [PageRange()]
class PageDiretory:

    def __init__(self, num_columns):
        #list of pageRanges
        #index (0 = within 5000 records, 1 = 5001 - 10000 records) based on config.PAGE_RANGE_LEN
        self.num_columns = num_columns
        self.pageRanges = [PageRange(num_columns)]
        self.currPageRangeIndex = 0

    def insertBaseRecord(self, record):
        currPageRange = self.pageRanges[self.currPageRangeIndex]

        if currPageRange.insertBaseRecord(record): 
            #successfully added a record into pageDir
            pass
        else: 
            #create new Page Range and add the record to the new Page Range
            self.addNewPageRange()
            currPageRange = self.pageRanges[self.currPageRangeIndex]
            currPageRange.insertBaseRecord(record)

    def insertTailRecord(self):
        pass

    def addNewPageRange(self):
        self.pageRanges.append(PageRange(self.num_columns))
        self.currPageRangeIndex += 1

    #input: RID
    #output: [type, pageRangeIndex, pageIndex]
       #type -> Base Page = 1 ; Tail Page = 2
    #pageRangeIndex -> 0 : Base Page 0; 1 : Base Page 1 (0 - how many Base Pages there are)
    #pageIndex -> the row index: 0 -> (PageSize - 1)
    def getRecordLocation(self, RID, head_flag):
        pass




