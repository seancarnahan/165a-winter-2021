from template.config import *
from template.page import Page
from template.lock_manager import LockManager


#has a physical page for every column in the table
class PhysicalPages:
    #TODO: pass down lock Manager
    def __init__(self, num_columns, lockManager: LockManager):
        self.physicalPages = []
        self.numOfRecords = 0
        self.lockManager = lockManager
        self.maxNumOfBasePages = 2

        for _ in range(num_columns+RECORD_COLUMN_OFFSET):
            self.physicalPages.append(Page())

    # record location = [recordType, locPRIndex, locBPIndex or locTPIndex]
    # returns the RID of the newly created Record
    def setPageRecord(self, record, recordLocation):
        # set Physical page location of recordLocation
        locPhyPageIndex = self.numOfRecords
        recordLocation.append(locPhyPageIndex)

        # keep attempting to find and lock an RID until successful
        didAcquireLock = False
        while not didAcquireLock:
            # create New RID with record Location
            RID = record.getNewRID(recordLocation[0], recordLocation[1], recordLocation[2], recordLocation[3])

            # attempt to acquire write lock
            didAcquireLock = self.lockManager.acquireWriteLock(RID)

            # if lock is not acquired move onto next record location
            if not didAcquireLock:
                # increment locPhyPageIndex
                recordLocation[3] += 1  # locPhyPageIndex

                # need to make a new base page or tail page
                if recordLocation[3] >= PAGE_SIZE:  # TODO: This might be an issue, double check i got this right
                    return False

        record.RID = RID
        # make check so tail record does not set self to base_RID
        if recordLocation[0] == 1:
            record.base_RID = record.RID

        self.physicalPages[INDIRECTION_COLUMN].write(record.indirection)
        self.physicalPages[RID_COLUMN].write(RID)
        self.physicalPages[TIMESTAMP_COLUMN].write(record.timestamp)
        self.physicalPages[SCHEMA_ENCODING_COLUMN].write(record.encoding)
        self.physicalPages[BASE_RID_COLUMN].write(record.base_RID)

        for col in range(RECORD_COLUMN_OFFSET, RECORD_COLUMN_OFFSET + len(record.columns)):
            columnData = record.columns[col - RECORD_COLUMN_OFFSET]

            self.physicalPages[col].write(columnData)

        self.numOfRecords += 1

        #TODO: release write lock at RID

        return RID

    def getPageRecord(self, record_index):
        """
        :param record_index: index of a record in the page (0-999)
        :return: List of int of length num_columns + RECORD_COLUMN_OFFSET
        """
        record = []

        for i in range(len(self.physicalPages)):
            record.append(self.physicalPages[i].getRecord(record_index))

        return record

    def getAllRecords(self):
        """
        :return: List of records (which are lists containing int)
        """
        page_records = []

        for record_index in range(self.numOfRecords):
            page_records.append(self.getPageRecord(record_index))

        return page_records

    def replaceRecord(self, record_index, record):
        """
        Replaces the values of a record for all data pages.

        :type record_index: int
        :param record_index: index within the page to replace at
        :type record: list
        :param record: List of size num_columns + RECORD_COLUMN_OFFSET
        """

        # do not replace meta data record values
        # note: possible point of failure here
        for col, page in enumerate(self.physicalPages):
            if col >= RECORD_COLUMN_OFFSET:
                self.physicalPages[col].replaceRecord(record_index, record[col])

    def hasCapacity(self):
        if self.numOfRecords >= PAGE_SIZE:
            #page is full
            return False
        else:
            #there is still room to add at least 1 more record
            return True
