from template.page import Page
from template.index import Index
from template.config import *

from time import time

class Record:

    #input: columns = list of integers
    def __init__(self, key, indirection, timestamp, encoding, columns):
        #not sure
        self.key = key

        #an RID if this is an update
        #set to NULL or NONE if this is the BASE record
        self.indirection = indirection

        #integer (based on milliseconds from epoch)
        self.timestamp = timestamp

        #bitmap
        self.encoding = encoding

        #list corresponding to all of the user created columns
        self.columns = columns

    #input: record location
    #output: record location in integer form
    def getNewRID(self, locType, locPRIndex, locBPIndex, locPhyPageIndex):
        return 123456789

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
        self.page_directory = PageDirectory(num_columns)
        self.index = Index(self)

        self.latestRID = None

        self.currPageRangeIndex = 0
        #self.currPageIndex = 0

    #INSERT -> only created new BASE Records
    #input: values: values of columns to be inserted; excluding the metadata
    def createNewRecord(self, key, columns):

        # RID = self.getNewRID() -> get RID when you put the record in the DB
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0

        #create Record Object
        record = Record(key, indirection, timeStamp, encoding, columns)

        #insert a new record -> all the checks for capacity are done implicitly
        self.page_directory.insertBaseRecord(record)

    #input: values: values of columns to be inserted; excluding the metadata
    #input: RID: the RID of the base Record you would like to provide an update for
    #  Given an RID of the record you would like to update
    #     - 1. first add the tail page to the correct Page Range and return the RID of the new tail page
    #     - 2. next go into the base page and change encoding to 1, and then change indirection to the new RID
    #     - note its up to the query to get the most updated values and combine them with the new values and the call updateRecord()
    def updateRecord(self, key, RID, values):

        #step 1: add tail page and return RID

        #create tail Record
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0
        record = Record(key, indirection, timeStamp, encoding, values)

        #add the record and get the RID
        tailRecordRID = self.page_directory.insertTailRecord(RID, record)

        #Step 2: update base page page with location of new tail record
        locType, locPRIndex, locBPIndex, locPhyPageIndex = self.page_directory.getRecordLocation(RID)
        physicalPages = self.page_directory.getPhysicalPages(locType, locPRIndex, locBPIndex, locPhyPageIndex)

        #set base page encoding to 1
        currEncoding = 1

        #set base page indirection to newly created tail page
        currIndirection = physicalPages[INDIRECTION_COLUMN].write(tailRecordRID)

    def getNewTimeStamp(self):
        return int(time())

#has a physical page for every column in the table
class PhysicalPages:
    def __init__(self, num_columns):
        self.physicalPages = []
        self.numOfRecords = 0

        for _ in range(num_columns):
            self.physicalPages.append(Page())

    # record location = [locType, locPRIndex, locBPIndex or locTPIndex]
    #returns the RID of the newly created Record
    def setPageRecord(self, record, recordLocation):
        #set last item of recordLocation
        locPhyPageIndex = self.numOfRecords - 1
        recordLocation.append(locPhyPageIndex)

        #create New RID with record Location
        RID = record.getNewRID(recordLocation[0], recordLocation[1], recordLocation[2], recordLocation[3])

        self.physicalPages[INDIRECTION_COLUMN].write(record.indirection)
        self.physicalPages[RID_COLUMN].write(RID)
        self.physicalPages[TIMESTAMP_COLUMN].write(record.timestamp)
        self.physicalPages[SCHEMA_ENCODING_COLUMN].write(record.encoding)

        for col in range(RECORD_COLUMN_OFFSET, RECORD_COLUMN_OFFSET + len(record.columns)):
            columnData = record.columns[col - RECORD_COLUMN_OFFSET]

            self.physicalPages[col].write(columnData)
            self.numOfRecords += 1
        
        return RID

    def hasCapacity(self):
        if self.numOfRecords >= PAGE_SIZE:
            #page is full
            return False
        else:
            #there is still room to add at least 1 more record
            return True

class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.maxNumOfBasePages = self.getPageRangeCapacity()
        self.currBasePageIndex = 0
        self.currTailPageIndex = 0
        
        #list of type BasePage
        self.basePages = [PhysicalPages(self.num_columns)]

        #list of type TailPage, when one tail page runs out add a new on to the list
        self.tailPages = [PhysicalPages(self.num_columns)]

    # record location = [locType, locPRIndex]
    def insertBaseRecord(self, record, recordLocation):
        currBasePage = self.basePages[self.currBasePageIndex]

        locBPIndex = self.currBasePageIndex

        if currBasePage.hasCapacity():
            recordLocation.append(locBPIndex)

            currBasePage.setPageRecord(record, recordLocation)
            return True #succesfully inserted a new record into Page Rage
        else:
            #check to if the Page Range can handle another base Page
            if self.addNewBasePage():
                #update location
                locBPIndex = self.currBasePageIndex
                recordLocation.append(locBPIndex)

                #write to the pages
                currBasePage.setPageRecord(record, recordLocation)
                return True #succesfully inserted a new record into Page Rage
            else:
                #there is no more room in Page Range, need to tell PageDir to make a new one
                return False #FAILED to insert a record into page Range

    # record location = [locType, locPRIndex]
    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, record, recordLocation):
        currTailPage = self.tailPages[self.currTailPageIndex]

        locTPIndex = self.currTailPageIndex

        if currTailPage.hasCapacity():
            recordLocation.append(locTPIndex)

            #write to the pages
            return currTailPage.setPageRecord(record, recordLocation)
        else:
            #create new TailPage()
            self.addNewTailPage()
            currTailPage = self.tailPages[self.currTailPageIndex]

            #update location
            locTPIndex = self.currBasePageIndex
            recordLocation.append(locTPIndex)

            #write to the pages
            return currTailPage.setPageRecord(record, recordLocation)

            
   

    def addNewBasePage(self):
        if self.hasCapacity():
            self.currBasePageIndex += 1
            self.basePages.append(PhysicalPages(self.num_columns))
            return True
        else:
            return False
  

    def addNewTailPage(self):
        self.currTailPageIndex += 1
        self.tailPages.append(PhysicalPages(self.num_columns))

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
class PageDirectory:

    def __init__(self, num_columns):
        #list of pageRanges
        #index (0 = within 5000 records, 1 = 5001 - 10000 records) based on config.PAGE_RANGE_LEN
        self.num_columns = num_columns
        self.pageRanges = [PageRange(num_columns)]
        self.currPageRangeIndex = 0

    def insertBaseRecord(self, record):
        #create new RID location
        locType = 1
        locPRIndex = self.currPageRangeIndex
        recordLocation = [locType, locPRIndex]

        currPageRange = self.pageRanges[self.currPageRangeIndex]

        if currPageRange.insertBaseRecord(record, recordLocation): 
            #successfully added a record into pageDir
            return True
        else: 
            #create new Page Range and add the record to the new Page Range
            self.addNewPageRange()
            currPageRange = self.pageRanges[self.currPageRangeIndex]

            #reset PRIndex location
            locPRIndex = self.currPageRangeIndex
            recordLocation = [locType, locPRIndex]

            currPageRange.insertBaseRecord(record, recordLocation)
            return True

    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, baseRID, record):
        baseRIDLoc = self.getRecordLocation(baseRID)

        #create new RID location
        locType = 2
        locPRIndex = baseRIDLoc[1]
        recordLocation = [locType, locPRIndex]

        #TODO: for merge -> we can get capacity of tail records, once it reaches its max then we can merge

        #PageRange that has the baseRID
        pageRange = self.pageRanges[locPRIndex]

        #set indirection and encoding of base RID 
        return pageRange.insertTailRecord(record, recordLocation)
        

    def addNewPageRange(self):
        self.pageRanges.append(PageRange(self.num_columns))
        self.currPageRangeIndex += 1

    #LocType, locPRIndex, locBPIndex or locTpIndex, locPhyPageIndex
    def getPhysicalPages(self, locType, locPRIndex, locBPIndex, locPhyPageIndex):
        if locType == 1:
            #base Page
            return self.pageRanges[locPRIndex].basePages[locBPIndex]
        else:
            #tail Page
            return self.pageRanges[locPRIndex].tailPages[locBPIndex]

    #input: RID
    #output: # record location = [locType, locPRIndex, locBPIndex or locTPIndex, locPhyPageIndex]
    def getRecordLocation(self, RID):
        return [1,23,45,6789]
        