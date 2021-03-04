from template.physical_pages import PhysicalPages
from template.config import *
from template.lock_manager import LockManager


class PageRange:

    def __init__(self, num_columns, table_name, page_range_index, lock_manager: LockManager):
        self.num_columns = num_columns
        self.maxNumOfBasePages = self.getPageRangeCapacity()
        self.currBasePageIndex = 0
        self.currTailPageIndex = 0
        self.id = page_range_index
        self.lock_manager = lock_manager


        #list of type BasePage
        self.basePages = [PhysicalPages(self.num_columns, self.lock_manager)]

        #list of type TailPage, when one tail page runs out add a new on to the list
        self.tailPages = [PhysicalPages(self.num_columns, self.lock_manager)]

    # record location = [recordType, locPRIndex]
    def insertBaseRecord(self, record, recordLocation):
        currBasePage = self.basePages[self.currBasePageIndex]
        locBPIndex = self.currBasePageIndex
        addNewBasePage = False

        if currBasePage.hasCapacity():
            recordLocation.append(locBPIndex)

            # if fails to add the record, then create a new base page
            if currBasePage.setPageRecord(record, recordLocation) == False:
                addNewBasePage = True

            return True # successfully inserted a new record into Page Rage
        else:
            addNewBasePage = True

        if addNewBasePage == True:
            #def acquirePageRangeLock(self, page_range_index: int) -> bool:
            #aquires the lock and appends the page
            #if: the base page that were trying to add is locked then wait
            #release

            # check to if the Page Range can handle another base Page
            if self.addNewBasePage(recordLocation):
                # update location
                locBPIndex = self.currBasePageIndex + 1
                recordLocation.append(locBPIndex)

                #write to the pages
                currBasePage = self.basePages[locBPIndex]

                # if fails to add the record throw error because this should be fresh
                if currBasePage.setPageRecord(record, recordLocation) == False:
                    print("ISSUE could not add a record to a fresh base page")
                return True  # successfully inserted a new record into Page Rage
            else:
                # there is no more room in Page Range, need to tell PageDir to make a new one
                return False  # FAILED to insert a record into page Range

    # record location = [recordType, locPRIndex]
    # returns the RID of the newly created Tail Record
    def insertTailRecord(self, record, recordLocation):
        currTailPage = self.tailPages[self.currTailPageIndex]
        locTPIndex = self.currTailPageIndex
        addNewTailPage = False

        if currTailPage.hasCapacity():
            recordLocation.append(locTPIndex)

            # if fails to add the record, then create a new tail page
            RID = currTailPage.setPageRecord(record, recordLocation)
            if RID == False:
                addNewTailPage = True
            else:
                return RID
        else:
            addNewTailPage = True

        if addNewTailPage == True:
            # update location
            locTPIndex = self.currTailPageIndex + 1
            recordLocation.append(locTPIndex)

            # create new TailPage()
            self.addNewTailPage(recordLocation)
            currTailPage = self.tailPages[locTPIndex]


            # if fails to add the record throw error because this should be fresh
            RID = currTailPage.setPageRecord(record, recordLocation)
            if RID == False:
                print("ISSUE could not add a record to a fresh tail page")

            return RID  # successfully inserted a new TAIL record into Page Rage

    def addNewBasePage(self, recordLocation):
        if self.hasCapacity():
            pr_index = recordLocation[1]
            didAcquirePRLock = self.lock_manager.acquirePageRangeLock(self, pr_index)

            #TODO: could be an issue here: if each overflow sequentially aquires the lock, depends how delayed the threads are
            if didAcquirePRLock:
                self.currBasePageIndex += 1
                self.basePages.append(PhysicalPages(self.num_columns, self.lock_manager))

            return True
        else:
            return False


    def addNewTailPage(self, recordLocation):
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
