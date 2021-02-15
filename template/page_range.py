from template.physical_pages import PhysicalPages
from template.config import *

class PageRange:

    def __init__(self, num_columns):
        self.num_columns = num_columns
        self.maxNumOfBasePages = self.getPageRangeCapacity()
        self.currBasePageIndex = 0
        self.currTailPageIndex = 0

        #keeps track of which base pages are in memory
        self.memoryTracker = []

        #
        self.dirtyBitTracker = []

        #list of type BasePage
        self.basePages = [PhysicalPages(self.num_columns)]

        #list of type TailPage, when one tail page runs out add a new on to the list
        self.tailPages = [PhysicalPages(self.num_columns)]

    # record location = [locType, locPRIndex]
    def insertBaseRecord(self, record, recordLocation):
        """
        Insert a base record

        :param record: the record object to insert
        :param recordLocation: [locType, locPRIndex]
        """
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
        """
        Insert a tail record

        :param record: the record object to insert
        :param recordLocation: [locType, locPRIndex]
        """
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
            locTPIndex = self.currTailPageIndex
            recordLocation.append(locTPIndex)

            #write to the pages
            return currTailPage.setPageRecord(record, recordLocation)

    def addNewBasePage(self):
        """
        Add a new base page to page range.
        :returns: True if successful, False otherwise
        """
        if self.hasCapacity():
            self.currBasePageIndex += 1
            self.basePages.append(PhysicalPages(self.num_columns))
            return True
        else:
            return False

    def addNewTailPage(self):
        """
        Add a new tail page to the page range
        """
        self.currTailPageIndex += 1
        self.tailPages.append(PhysicalPages(self.num_columns))

    #figure out how many many base pages can fit into PAGE RANGE without exceeding MAX_PAGE_RANGE_SIZE
    #return number of Base Pages
    def getPageRangeCapacity(self):
        """ Determine how many base pages can be added into page range """
        maxBasePageSize = self.num_columns

        while((maxBasePageSize + self.num_columns) <=  MAX_PAGE_RANGE_SIZE):
            maxBasePageSize += self.num_columns

        return maxBasePageSize / self.num_columns

    #True if Page Range has capacity for another Base Page, if not PageDir should create another PageRange
    def hasCapacity(self):
        """ Returns true if a base page can be added otherwise return False """
        if len(self.basePages) >= self.maxNumOfBasePages:
            return False
        else:
            return True
