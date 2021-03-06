from template.index import Index
from template.page_directory import PageDirectory
from template.record import Record
from template.config import *
from template.lock_manager import LockManager

from time import time


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, parent, table_name, num_columns, key, bufferPool):
        self.parent_db = parent
        self.table_name = table_name
        self.key = key
        self.num_columns = num_columns
        self.num_all_columns = num_columns + RECORD_COLUMN_OFFSET
        self.page_directory = PageDirectory(self.num_all_columns, bufferPool, table_name)
        self.index = Index(self)
        self.index.create_index(key + RECORD_COLUMN_OFFSET)
        self.lock_manager = LockManager()

    #SUM SELECT
    # Input: RID
    # Output: Record Object with RID added
    def getRecord(self, RID):
        recordType, locPRIndex, loc_PIndex, locPhyPageIndex = self.page_directory.getRecordLocation(RID)

        # load page range into buffer pool
        pageRange = self.page_directory.bufferPool.loadPageRange(self.table_name, locPRIndex)

        physicalPages = None
        if recordType == 1:
            physicalPages = pageRange.basePages[loc_PIndex]
        elif recordType == 2:
            physicalPages = pageRange.tailPages[loc_PIndex]

        indirection = physicalPages.physicalPages[INDIRECTION_COLUMN].getRecord(locPhyPageIndex)
        RID = physicalPages.physicalPages[RID_COLUMN].getRecord(locPhyPageIndex)
        timeStamp = physicalPages.physicalPages[TIMESTAMP_COLUMN].getRecord(locPhyPageIndex)
        encoding = physicalPages.physicalPages[SCHEMA_ENCODING_COLUMN].getRecord(locPhyPageIndex)
        key = physicalPages.physicalPages[self.key + RECORD_COLUMN_OFFSET].getRecord(locPhyPageIndex)

        columns = []

        for i in range(RECORD_COLUMN_OFFSET, self.num_all_columns):
            columns.append(physicalPages.physicalPages[i].getRecord(locPhyPageIndex))

        self.page_directory.bufferPool.releasePin(self.table_name, locPRIndex)
        record = Record(key, indirection, timeStamp, encoding, columns)
        record.RID = RID

        return record

    """
    return the latest updated tail record of the given base RID
    :param baseRID: integer     #rid of the base page you want the latest updates for
    """

    def getLatestupdatedRecord(self, baseRID):
        record = self.getRecord(baseRID)

        while (record.encoding != 0):
            record = self.getRecord(record.indirection)

        return record

    # INSERT
    # insert -> only created new BASE Records
    # input: values: values of columns to be inserted; excluding the metadata
    def createNewRecord(self, key, columns):

        # RID = self.getNewRID() -> get RID when you put the record in the DB
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0

        # create Record Object
        record = Record(key, indirection, timeStamp, encoding, columns)

        # insert a new record -> all the checks for capacity are done implicitly
        self.page_directory.insertBaseRecord(record, self.lock_manager)

        self.index.insert(record.RID, columns)

    #DELETE UPDATE
    # input: values: values of columns to be inserted; excluding the metadata
    # input: RID: the RID of the base Record you would like to provide an update for
    def updateRecord(self, key, RID, values, deleteFlag=False):

        # Step 1: get the updated Values
        baseRecord = self.getRecord(RID)
        prevUpdateRecord = None
        updatedValues = None
        prevRecordColData = None # This is used for abort in transaction

        if baseRecord.indirection == 0:
            # base Record has not been updated
            prevRecordColData = baseRecord.columns  # saved for abort

            updatedValues = self.getUpdatedRow(prevRecordColData, values)
        else:
            # base Record has been updated
            prevUpdateRecord = self.getRecord(baseRecord.indirection)
            prevRecordColData = prevUpdateRecord.columns  # saved for abort

            updatedValues = self.getUpdatedRow(prevRecordColData, values)

        if prevUpdateRecord is not None:
            self.index.updateIndexes(baseRecord.RID, prevUpdateRecord.columns, updatedValues)
        else:
            self.index.updateIndexes(baseRecord.RID, baseRecord.columns, updatedValues)

        # step 2: create New tail Record
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0
        base_rid = baseRecord.RID

        # check for delete flag
        if deleteFlag == True:
            encoding = 2
            self.index.remove(RID, updatedValues)
            for i in range(len(updatedValues)):
                updatedValues[i] = 0

        record = Record(key, indirection, timeStamp, encoding, updatedValues)
        record.base_RID = base_rid

        # step 3: add the record and get the RID
        tailRecordRID = self.page_directory.insertTailRecord(RID, record, self.lock_manager)

        # step 4: if there is a prevTail, then set the prev tail record to point to the new tail record
        if baseRecord.indirection != 0:
            recordType, locPRIndex, locTPIndex, locPhyPageIndex = self.page_directory.getRecordLocation(
                prevUpdateRecord.RID)
            prevTailRecordPhysicalPages = self.page_directory.getPhysicalPages(recordType, locPRIndex, locTPIndex,
                                                                               locPhyPageIndex).physicalPages
            prevTailRecordPhysicalPages[INDIRECTION_COLUMN].replaceRecord(locPhyPageIndex, tailRecordRID)
            prevTailRecordPhysicalPages[SCHEMA_ENCODING_COLUMN].replaceRecord(locPhyPageIndex, 1)
            self.page_directory.bufferPool.releasePin(self.table_name, locPRIndex)

        #Step 5: update base page with location of new tail record
        recordType, locPRIndex, locBPIndex, locPhyPageIndex = self.page_directory.getRecordLocation(RID)
        basePagePhysicalPages = self.page_directory.getPhysicalPages(recordType, locPRIndex, locBPIndex,
                                                                     locPhyPageIndex).physicalPages
        basePagePhysicalPages[INDIRECTION_COLUMN].replaceRecord(locPhyPageIndex, tailRecordRID)
        basePagePhysicalPages[SCHEMA_ENCODING_COLUMN].replaceRecord(locPhyPageIndex, 1)
        self.page_directory.bufferPool.releasePin(self.table_name, locPRIndex)

        # saved for abort
        return prevRecordColData



    #input currValues and update should both be lists of integers of equal lengths
    #output: for any value in update that is not "none", that value will overwrite the corresponding currValues, and then return this new list
    def getUpdatedRow(self, currValues, update):
        updatedValues = []

        for i in range(len(currValues)):
            if update[i] is None:
                updatedValues.append(currValues[i])
            else:
                updatedValues.append(update[i])

        return updatedValues

    def getNewTimeStamp(self):
        return int(time())
