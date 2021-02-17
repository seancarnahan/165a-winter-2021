from template.page_range import PageRange

class PageDirectory:

    #index (0 = within 5000 records, 1 = 5001 - 10000 records) based on config.PAGE_RANGE_LEN
    def __init__(self, num_columns, bufferPool, table_index):
        self.num_columns = num_columns
        self.bufferPool = bufferPool
        self.table_index = table_index

    def loadPageRange(self, page_range_index):
        curr_pagerange_index_in_bufferPool = self.bufferPool.getCurrPageRangeIndex(self.table_index)

        if curr_pagerange_index_in_bufferPool != page_range_index:
            self.bufferPool.requestNewPageRange(self.table_index, page_range_index)

        return self.bufferPool.getPageRange()

    def insertBaseRecord(self, record):
        #create new RID location
        locType = 1
        locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_index)
        recordLocation = [locType, locPRIndex]

        currPageRange = self.bufferPool.getPageRange()

        if currPageRange.insertBaseRecord(record, recordLocation):
            #successfully added a record into pageRange that is loaded in buffer pool
            return True
        else:
            #Page Range is full: ask buffer Pool to initialize a new Page Range
            self.bufferPool.addNewPageRange()
            currPageRange = self.bufferPool.getPageRange()

            #reset PRIndex location
            locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_index)
            recordLocation = [locType, locPRIndex]

            currPageRange.insertBaseRecord(record, recordLocation)
            return True


    #TODO: for merge -> we can get capacity of tail records, once it reaches its max then we can merge
    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, baseRID, record):
        baseRIDLoc = self.getRecordLocation(baseRID)

        #create new RID location
        locType = 2
        locPRIndex = baseRIDLoc[1]
        recordLocation = [locType, locPRIndex]

        #load pageRange into bufferPool if needed
        currPageRange = self.loadPageRange(locPRIndex)
        
        #set indirection and encoding of base RID
        return currPageRange.insertTailRecord(record, recordLocation)


    #LocType, locPRIndex, locBPIndex or locTpIndex, locPhyPageIndex
    def getPhysicalPages(self, locType, locPRIndex, loc_PIndex, locPhyPageIndex):
        #load pageRange
        pageRange = self.loadPageRange(locPRIndex)

        if locType == 1:
            #base Page
            return pageRange.basePages[loc_PIndex]
        else:
            #tail Page
            return pageRange.tailPages[loc_PIndex]

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
