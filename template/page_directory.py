from template.page_range import PageRange

class PageDirectory:

    #index (0 = within 5000 records, 1 = 5001 - 10000 records) based on config.PAGE_RANGE_LEN
    def __init__(self, num_columns, bufferPool, table_name):
        self.num_columns = num_columns
        self.bufferPool = bufferPool
        self.table_name = table_name

    def loadPageRange(self, page_range_index):
        curr_pagerange_index_in_bufferPool = self.bufferPool.getCurrPageRangeIndex(self.table_name)

        if curr_pagerange_index_in_bufferPool != page_range_index:
            self.bufferPool.requestPageRange(self.table_name, page_range_index)

        # get_page_range_from_buffer_pool
        return self.bufferPool.getPageRange()

    def insertBaseRecord(self, record):
        #create new RID location
        recordType = 1
        locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_name)
        recordLocation = [recordType, locPRIndex]

        currPageRange = self.bufferPool.getPageRange()

        if currPageRange.insertBaseRecord(record, recordLocation):
            #successfully added a record into pageRange that is loaded in buffer pool
            return True
        else:
            #Page Range is full: ask buffer Pool to initialize a new Page Range
            self.bufferPool.addNewPageRange()
            currPageRange = self.bufferPool.getPageRange()

            #reset PRIndex location
            locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_name)
            recordLocation = [recordType, locPRIndex]

            currPageRange.insertBaseRecord(record, recordLocation)
            return True


    #TODO: for merge -> we can get capacity of tail records, once it reaches its max then we can merge
    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, baseRID, record):
        baseRIDLoc = self.getRecordLocation(baseRID)

        #create new RID location
        recordType = 2
        locPRIndex = baseRIDLoc[1]
        recordLocation = [recordType, locPRIndex]

        #load pageRange into bufferPool if needed
        currPageRange = self.loadPageRange(locPRIndex)

        #set indirection and encoding of base RID
        return currPageRange.insertTailRecord(record, recordLocation)


    #recordType, locPRIndex, locBPIndex or locTpIndex, locPhyPageIndex
    def getPhysicalPages(self, recordType, locPRIndex, loc_PIndex, locPhyPageIndex):
        #load pageRange
        pageRange = self.loadPageRange(locPRIndex)

        if recordType == 1:
            #base Page
            return pageRange.basePages[loc_PIndex]
        else:
            #tail Page
            return pageRange.tailPages[loc_PIndex]

    #input: RID
    #output: # record location = [recordType, locPRIndex, locBPIndex or locTPIndex, locPhyPageIndex]; EX: [1,23,45,6789]
    # CONSIDER: using Bitwise operations to improve speed
    def getRecordLocation(self, RID):
        locPhyPageIndex = RID % 10000
        RID //= 10000
        lock_PIndex = RID % 100
        RID //=100
        locPRIndex = RID % 100
        RID //= 100

        #RID => recordType
        return [RID, locPRIndex, lock_PIndex, locPhyPageIndex]
