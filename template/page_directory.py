from template.page_range import PageRange

class PageDirectory:

    #index (0 = within 5000 records, 1 = 5001 - 10000 records) based on config.PAGE_RANGE_LEN
    def __init__(self, num_columns, bufferPool, table_name):
        self.num_columns = num_columns
        self.bufferPool = bufferPool
        self.table_name = table_name

    #returns True when its a success
    def insertBaseRecord(self, record):
        #record type is basePage
        recordType = 1

        #get available pageRange for insert
        locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_name)

        #initialize record location
        recordLocation = [recordType, locPRIndex]

        #points to the correct PageRange in BufferPool
        currPageRange = self.bufferPool.loadPageRange(self.table_name, locPRIndex)

        #attempt to add record PageRange
        if currPageRange.insertBaseRecord(record, recordLocation):
            #successfully added a record into pageRange that is loaded in buffer pool
            return True
        else:
            #Page Range is full: ask buffer Pool to initialize a new Page Range
            self.bufferPool.addNewPageRangeToDisk()

            #get the new Page Range Index
            locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_name)

            #reset record location
            recordLocation = [recordType, locPRIndex]

            #points to the correct PageRange in BufferPool
            currPageRange = self.bufferPool.loadPageRange(self.table_name, locPRIndex)

            #Recursive -> attempt to add record to PageRange
            currPageRange.insertBaseRecord(record, recordLocation)

            return True


    #TODO: for merge -> we can get capacity of tail records, once it reaches its max then we can merge
    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, baseRID, record):

        #List of elements that make up the RID
        baseRIDLoc = self.getRecordLocation(baseRID)

        #record type is set to Tail
        recordType = 2

        #get page range index from baseRIDLoc
        locPRIndex = baseRIDLoc[1]

        #initialize record location
        recordLocation = [recordType, locPRIndex]

        #load pageRange into bufferPool if needed
        currPageRange = self.bufferPool.loadPageRange(self.table_name, locPRIndex)

        #try to add record to PageRange
        return currPageRange.insertTailRecord(record, recordLocation)


    def getPhysicalPages(self, recordType, locPRIndex, loc_PIndex, locPhyPageIndex):
        #load pageRange
        pageRange = self.loadPageRange(self.table_name, locPRIndex)

        if recordType == 1:
            #base Page
            return pageRange.basePages[loc_PIndex]
        else:
            #tail Page
            return pageRange.tailPages[loc_PIndex]

    #input: RID
    #output: # record location = [recordType, locPRIndex, locBPIndex or locTPIndex, locPhyPageIndex]; EX: [1,23,45,6789]
    def getRecordLocation(self, RID):
        locPhyPageIndex = RID % 10000
        RID //= 10000
        lock_PIndex = RID % 100
        RID //=100
        locPRIndex = RID % 100
        RID //= 100

        #RID => recordType
        return [RID, locPRIndex, lock_PIndex, locPhyPageIndex]
