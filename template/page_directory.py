from template.lock_manager import LockManager

class PageDirectory:

    #index (0 = within 5000 records, 1 = 5001 - 10000 records) based on config.PAGE_RANGE_LEN
    def __init__(self, num_columns, bufferPool, table_name):
        self.num_columns = num_columns
        self.bufferPool = bufferPool
        self.table_name = table_name

    #the page range has been updated, update the dirty bit tracker
    def update_page_range_dirty_bit_tracker(self, page_range_index):
        with self.bufferPool.bp_lock:
            pr_buffer_pool_index = self.bufferPool.get_page_range_index_in_buffer_pool(self.table_name, page_range_index)
            self.bufferPool.dirtyBitTracker[pr_buffer_pool_index] = True

    #returns True when its a success
    def insertBaseRecord(self, record, lock_manager: LockManager):
        #record type is basePage
        recordType = 1

        #get available pageRange for insert
        locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_name)

        if locPRIndex == -1:
            locPRIndex = 0

        #initialize record location
        recordLocation = [recordType, locPRIndex]

        #points to the correct PageRange in BufferPool
        currPageRange = self.bufferPool.loadPageRange(self.table_name, locPRIndex)

        #attempt to add record PageRange
        if currPageRange.insertBaseRecord(record, recordLocation, lock_manager):
            # used for commit: update the number of records in
            records_in_PR_index = self.bufferPool.get_tailRecordsSinceLastMerge_index(self.table_name,
                                                                                      locPRIndex)
            self.bufferPool.recordsInPageRange[records_in_PR_index][2] += 1

            #unload the pin
            self.bufferPool.releasePin(self.table_name, locPRIndex)

            # update dirty bit tracker
            self.update_page_range_dirty_bit_tracker(locPRIndex)

            # successfully added a record into pageRange that is loaded in buffer pool
            return True
        else:
            #if it fails unload the pin
            self.bufferPool.releasePin(self.table_name, locPRIndex)

            # acquireTableLock(self, page_range_index: int)
            lock_manager.acquireTableLock()

            # check if PR has already been created
            if self.bufferPool.currPageRangeIndexes[self.table_name] <= locPRIndex:
                # Page Range is full: ask buffer Pool to initialize a new Page Range
                self.bufferPool.addNewPageRangeToDisk(self.table_name)

            # release lock for the table
            lock_manager.releaseTableLock()

            #get the new Page Range Index
            locPRIndex = self.bufferPool.getCurrPageRangeIndex(self.table_name)

            #reset record location
            recordLocation = [recordType, locPRIndex]

            #points to the correct PageRange in BufferPool
            currPageRange = self.bufferPool.loadPageRange(self.table_name, locPRIndex)

            #Recursive -> attempt to add record to PageRange
            currPageRange.insertBaseRecord(record, recordLocation, lock_manager)

            # used for commit: update the number of records in
            records_in_PR_index = self.bufferPool.get_tailRecordsSinceLastMerge_index(self.table_name, locPRIndex)

            self.bufferPool.recordsInPageRange[records_in_PR_index][2] += 1

            #unload the pin
            self.bufferPool.releasePin(self.table_name, locPRIndex)

            # update dirty bit tracker
            self.update_page_range_dirty_bit_tracker(locPRIndex)

            return True


    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, baseRID, record, lock_manager: LockManager):

        #List of elements that make up the RID
        baseRIDLoc = self.getRecordLocation(baseRID)

        #record type is set to Tail
        recordType = 2

        #get page range index from baseRIDLoc
        locPRIndex = baseRIDLoc[1]

        #initialize record location
        recordLocation = [recordType, locPRIndex]

        #update dirty bit tracker
        self.update_page_range_dirty_bit_tracker(locPRIndex)

        #load pageRange into bufferPool if needed
        currPageRange = self.bufferPool.loadPageRange(self.table_name, locPRIndex)

        #try to add record to PageRange
        ridOfTailRecord = currPageRange.insertTailRecord(record, recordLocation, lock_manager)

        tailRecordsSinceLastMergeIndex = self.bufferPool.get_tailRecordsSinceLastMerge_index(self.table_name, locPRIndex)
        self.bufferPool.tailRecordsSinceLastMerge[tailRecordsSinceLastMergeIndex][2] += 1
        self.bufferPool.recordsInPageRange[tailRecordsSinceLastMergeIndex][2] += 1

        #decrement pin
        self.bufferPool.releasePin(self.table_name, locPRIndex)

        # update dirty bit tracker
        self.update_page_range_dirty_bit_tracker(locPRIndex)

        return ridOfTailRecord


    def getPhysicalPages(self, recordType, locPRIndex, loc_PIndex, locPhyPageIndex):
        #load pageRange
        pageRange = self.bufferPool.loadPageRange(self.table_name, locPRIndex)


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
