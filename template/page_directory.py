from template.page_range import PageRange

#PageDirectory = [PageRange()]
class PageDirectory:

    def __init__(self, num_columns):
        #list of pageRanges
        #index (0 = within 5000 records, 1 = 5001 - 10000 records) based on config.PAGE_RANGE_LEN
        self.num_columns = num_columns
        self.pageRanges = [PageRange(num_columns)]
        # self.pageRanges = BufferPool([PageRange(num_columns)])
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
