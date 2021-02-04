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
        #set to 0 if this is the BASE record
        self.indirection = indirection

        #integer (based on milliseconds from epoch)
        self.timestamp = timestamp

        #bitmap
        self.encoding = encoding

        #list corresponding to all of the user created columns
        self.columns = columns

        #must call getNewRID for this to be added
        self.RID = None

    #input: record location
    #output: record location in integer form; #example: 123456789
    def getNewRID(self, locType, locPRIndex, locBPIndex, locPhyPageIndex):
        num = locType*(10**8) + locPRIndex*(10**6) + locBPIndex*(10**4) + locPhyPageIndex

        return num        

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
        self.page_directory = PageDirectory(self.num_columns)
        self.index = Index(self)
        self.index.create_index(key+RECORD_COLUMN_OFFSET)
        self.latestRID = None

        self.currPageRangeIndex = 0
    
    #Input: RID
    #Output: Record Object with RID added
    def getRecord(self, RID):
        locType, locPRIndex, lock_PIndex, locPhyPageIndex = self.page_directory.getRecordLocation(RID)
        pageRange = self.page_directory.pageRanges[locPRIndex]

        physicalPages = None
        if locType == 1:
            physicalPages = pageRange.basePages[lock_PIndex]
        elif locType == 2:
            physicalPages = pageRange.tailPages[lock_PIndex]

        indirection = physicalPages.physicalPages[INDIRECTION_COLUMN].getRecord(locPhyPageIndex)
        RID = physicalPages.physicalPages[RID_COLUMN].getRecord(locPhyPageIndex)
        timeStamp = physicalPages.physicalPages[TIMESTAMP_COLUMN].getRecord(locPhyPageIndex)
        encoding = physicalPages.physicalPages[SCHEMA_ENCODING_COLUMN].getRecord(locPhyPageIndex)
        key = physicalPages.physicalPages[KEY_COLUMN].getRecord(locPhyPageIndex)

        columns = []

        for i in range(RECORD_COLUMN_OFFSET, self.num_columns):
            columns.append(physicalPages.physicalPages[i].getRecord(locPhyPageIndex))

        record = Record(key, indirection, timeStamp, encoding, columns)
        record.RID = RID

        return record
        

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

        self.index.insert(record.RID, columns)

    #input: values: values of columns to be inserted; excluding the metadata
    #input: RID: the RID of the base Record you would like to provide an update for
    def updateRecord(self, key, RID, values):
        #Step 1: get the updated Values
        baseRecord = self.getRecord(RID)
        prevUpdateRecord = None
        updatedValues = None

        if baseRecord.indirection == 0:
            #base Record has not been updated

            updatedValues = self.getUpdatedRow(baseRecord.columns, values)
        else:
            #base Record has been updated
            prevUpdateRecord = self.getRecord(baseRecord.indirection)

            updatedValues = self.getUpdatedRow(prevUpdateRecord.columns, values)

        self.index.updateIndexes(baseRecord.RID, baseRecord.columns, updatedValues)
        #step 2: add tail page and return RID

        #create New tail Record
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0
        record = Record(key, indirection, timeStamp, encoding, updatedValues)

        #add the record and get the RID
        tailRecordRID = self.page_directory.insertTailRecord(RID, record)

        #step 3: if there is a prevTail, then set the prev tail record to point to the new tail record
        if baseRecord.indirection != 0:
            locType, locPRIndex, locTPIndex, locPhyPageIndex = self.page_directory.getRecordLocation(prevUpdateRecord.RID)
            prevTailRecordPhysicalPages = self.page_directory.getPhysicalPages(locType, locPRIndex, locTPIndex, locPhyPageIndex).physicalPages
            prevTailRecordPhysicalPages[INDIRECTION_COLUMN].write(tailRecordRID)
            prevTailRecordPhysicalPages[SCHEMA_ENCODING_COLUMN].write(1)

        #Step 4: update base page with location of new tail record
        locType, locPRIndex, locBPIndex, locPhyPageIndex = self.page_directory.getRecordLocation(RID)
        basePagePhysicalPages = self.page_directory.getPhysicalPages(locType, locPRIndex, locBPIndex, locPhyPageIndex).physicalPages
        basePagePhysicalPages[INDIRECTION_COLUMN].write(tailRecordRID)
        basePagePhysicalPages[SCHEMA_ENCODING_COLUMN].write(1)

    #input currValues and update should both be lists of integers of equal lengths
    #output: for any value in update that is not "none", that value will overwrite the corresponding currValues, and then return this new list
    def getUpdatedRow(self, currValues, update):
        updatedValues = []

        for i in range(len(currValues)):
            if update[i] == None:
                updatedValues.append(currValues[i])
            else:
                updatedValues.append(update[i])

        return updatedValues

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
        locPhyPageIndex = self.numOfRecords 
        recordLocation.append(locPhyPageIndex)

        #create New RID with record Location
        RID = record.getNewRID(recordLocation[0], recordLocation[1], recordLocation[2], recordLocation[3])

        self.physicalPages[INDIRECTION_COLUMN].write(record.indirection)
        self.physicalPages[RID_COLUMN].write(RID)
        self.physicalPages[TIMESTAMP_COLUMN].write(record.timestamp)
        self.physicalPages[SCHEMA_ENCODING_COLUMN].write(record.encoding)
        self.physicalPages[KEY_COLUMN].write(record.key)

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
                currBasePage = self.basePages[self.currBasePageIndex]
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
    #output: # record location = [locType, locPRIndex, locBPIndex or locTPIndex, locPhyPageIndex]; EX: [1,23,45,6789]
    # CONSIDER: using Bitwise operations to improve speed
    def getRecordLocation(self, RID):
        locPhyPageIndex = RID % 10000
        RID //= 10000
        lock_PIndex = RID % 100
        RID //=100
        locPRIndex = RID % 100
        RID //= 100

        #RID => locType
        return [RID, locPRIndex, lock_PIndex, locPhyPageIndex]
        