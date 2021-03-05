from template.physical_pages import PhysicalPages
from template.config import *
from template.lock_manager import LockManager

class PageRange:

    def __init__(self, num_columns, table_name, page_range_index):
        self.num_columns = num_columns
        self.maxNumOfBasePages = self.getPageRangeCapacity()
        self.currBasePageIndex = 0
        self.currTailPageIndex = 0
        self.id = page_range_index

        self.takenBasePages = [0]
        self.takenTailPages = [0]

        #list of type BasePage
        self.basePages = [PhysicalPages(self.num_columns)]

        #list of type TailPage, when one tail page runs out add a new on to the list
        self.tailPages = [PhysicalPages(self.num_columns)]

    # record location = [recordType, locPRIndex]
    def insertBaseRecord(self, record, recordLocation, lock_manager: LockManager):
        currBasePage = self.basePages[self.currBasePageIndex]
        locBPIndex = self.currBasePageIndex
        nextBasePage = False

        if currBasePage.hasCapacity():
            recordLocation.append(locBPIndex)

            # if fails to add the record, then create a new base page
            if currBasePage.setPageRecord(record, recordLocation, lock_manager) == False:
                nextBasePage = True
            else:
                return True  # successfully inserted a new record into Page Rage
        else:
            nextBasePage = True

        if nextBasePage:
            # set new record location
            locBPIndex += 1
            recordLocation.append(locBPIndex)

            # new Page Range needs to be created
            if locBPIndex > 1:
                return False

            # If locked, then wait
            while self.lock_manager.acquirePageRangeLock(recordLocation[1]):
                continue

            if locBPIndex in self.takenBasePages:
                # necessary base page is already created
                pass
            else:
                #create a new base page
                #TODO: when this returns False, avoiding race condition for created page range
                self.addNewBasePage(locBPIndex)

            #TODO: release write lock on page range

            currBasePage = self.basePages[locBPIndex]

            # if fails to add the record throw error because this should be fresh
            if currBasePage.setPageRecord(record, recordLocation, lock_manager) == False:
                print("ISSUE could not add a record to a fresh base page")

    # record location = [recordType, locPRIndex]
    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, record, recordLocation, lock_manager: LockManager):
        currTailPage = self.tailPages[self.currTailPageIndex]
        locTPIndex = self.currTailPageIndex
        nextTailPage = False

        if currTailPage.hasCapacity():
            recordLocation.append(locTPIndex)

            # if fails to add the record, then create a new tail page
            RID = currTailPage.setPageRecord(record, recordLocation, lock_manager)
            if RID == False:
                nextTailPage = True
            else:
                return RID
        else:
            nextTailPage = True

        if nextTailPage:
            # update location
            locTPIndex = self.currTailPageIndex
            recordLocation.append(locTPIndex)

            # If locked, then wait
            while self.lock_manager.acquirePageRangeLock(recordLocation[1]):
                continue

            if locTPIndex in self.takenTailPages:
                # necessary tail page is already created
                pass
            else:
                #create a new tail page
                self.addNewTailPage(locTPIndex)

            #TODO release write page range lock

            currTailPage = self.tailPages[locTPIndex]

            # set page record
            RID = currTailPage.setPageRecord(record, recordLocation, lock_manager)
            if RID == False:
                print("ISSUE could not add a record to a fresh tail page")

            return RID  # successfully inserted a new TAIL record into Page Rage

    def addNewBasePage(self, locBPIndex):
        if self.hasCapacity():
            self.takenBasePages.append(locBPIndex)
            self.currBasePageIndex += 1
            self.basePages.append(PhysicalPages(self.num_columns, self.lock_manager))

            return True
        else:
            return False


    def addNewTailPage(self, locTPIndex):
        self.takenBasePages.append(locTPIndex)
        self.currTailPageIndex += 1
        self.tailPages.append(PhysicalPages(self.num_columns, self.lock_manager))

    #figure out how many many base pages can fit into PAGE RANGE without exceeding MAX_PAGE_RANGE_SIZE
    #return number of Base Pages
    def getPageRangeCapacity(self):
        maxBasePageSize = self.num_columns

        while((maxBasePageSize + self.num_columns) <=  MAX_PAGE_RANGE_SIZE):
            maxBasePageSize += self.num_columns

        # return maxBasePageSize / self.num_columns
        return 2

    #True if Page Range has capacity for another Base Page, if not PageDir should create another PageRange
    def hasCapacity(self):
        if len(self.basePages) >= self.maxNumOfBasePages:
            return False
        else:
            return True
