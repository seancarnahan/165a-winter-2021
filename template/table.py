from template.page import Page
from template.index import Index
from template.page_directory import PageDirectory
from template.record import Record
from template.config import *

from time import time


class Table:

    def __init__(self, name, num_columns, key, bufferPool):
        """
        :param name: string         #Table name
        :param num_columns: int     #Number of Columns: all columns are integer
        :param key: int             #Index of table key in columns
        """
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_all_columns = num_columns + RECORD_COLUMN_OFFSET
        self.page_directory = PageDirectory(self.num_all_columns, bufferPool)
        self.index = Index(self)
        self.index.create_index(key + RECORD_COLUMN_OFFSET)
        self.latestRID = None

        self.currPageRangeIndex = 0

    def getRecord(self, RID):
        """
        Get a record object with matching RID

        :param RID: int.        The rid of the record to retrieve
        """
        locType, locPRIndex, lock_PIndex, locPhyPageIndex = self.page_directory.getRecordLocation(RID)
        pageRange = self.page_directory.pageRanges[locPRIndex]

        physicalPages = None
        if locType == 1:
            physicalPages = pageRange.basePages[lock_PIndex]
        elif locType == 2:
            physicalPages = pageRange.tailPages[lock_PIndex]

        indirection = physicalPages.physicalPages[INDIRECTION_COLUMN].getRecord(locPhyPageIndex)
        RID = physicalPages.physicalPages[RID_COLUMN].getRecord(locPhyPageIndex)
        timeStamp = physicalPages.physicalPages[TIMESTAMP_COLUMN].getRecord(locPhyPageIndex)
        encoding = physicalPages.physicalPages[SCHEMA_ENCODING_COLUMN].getRecord(locPhyPageIndex)
        key = physicalPages.physicalPages[self.key + RECORD_COLUMN_OFFSET].getRecord(locPhyPageIndex)

        columns = []

        for i in range(RECORD_COLUMN_OFFSET, self.num_all_columns):
            columns.append(physicalPages.physicalPages[i].getRecord(locPhyPageIndex))

        record = Record(key, indirection, timeStamp, encoding, columns)
        record.RID = RID

        return record

    def getLatestupdatedRecord(self, baseRID):
        """
        Get the latest version of the record with baseRID

        :param baseRID: int. the RID of the base record you want.
        """
        record = self.getRecord(baseRID)

        while (record.encoding != 0):
            record = self.getRecord(record.indirection)

        return record

    def createNewRecord(self, key, columns):
        """
        Add a new record into the table.

        :param key: int.    the primary key of the record
        :param columns: list. the user data associated with the primary key.
        """

        # RID = self.getNewRID() -> get RID when you put the record in the DB
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0

        # create Record Object
        record = Record(key, indirection, timeStamp, encoding, columns)

        # insert a new record -> all the checks for capacity are done implicitly
        self.page_directory.insertBaseRecord(record)

        self.index.insert(record.RID, columns)

    def updateRecord(self, key, RID, values, deleteFlag=False):
        """
        Update a record in the table.

        :param key: int. the primary key of the record to update
        :param RID: int. the base record RID
        :param values: list. the new set of values to replace the old values with
        :param deleteFlag: boolean. If true, will mark the base record as deleted. If False, will update the record.
        """

        # Step 1: get the updated Values
        baseRecord = self.getRecord(RID)
        prevUpdateRecord = None
        updatedValues = None

        if baseRecord.indirection == 0:
            # base Record has not been updated

            updatedValues = self.getUpdatedRow(baseRecord.columns, values)
        else:
            # base Record has been updated
            prevUpdateRecord = self.getRecord(baseRecord.indirection)

            updatedValues = self.getUpdatedRow(prevUpdateRecord.columns, values)

        if prevUpdateRecord is not None:
            self.index.updateIndexes(baseRecord.RID, prevUpdateRecord.columns, updatedValues)
        else:
            self.index.updateIndexes(baseRecord.RID, baseRecord.columns, updatedValues)

        # step 2: create New tail Record
        indirection = 0
        timeStamp = self.getNewTimeStamp()
        encoding = 0

        # check for delete flag
        if deleteFlag == True:
            encoding = 2
            for i in range(len(updatedValues)):
                updatedValues[i] = 0

        record = Record(key, indirection, timeStamp, encoding, updatedValues)

        # step 3: add the record and get the RID
        tailRecordRID = self.page_directory.insertTailRecord(RID, record)

        # step 4: if there is a prevTail, then set the prev tail record to point to the new tail record
        if baseRecord.indirection != 0:
            locType, locPRIndex, locTPIndex, locPhyPageIndex = self.page_directory.getRecordLocation(
                prevUpdateRecord.RID)
            prevTailRecordPhysicalPages = self.page_directory.getPhysicalPages(locType, locPRIndex, locTPIndex,
                                                                               locPhyPageIndex).physicalPages
            prevTailRecordPhysicalPages[INDIRECTION_COLUMN].replaceRecord(locPhyPageIndex, tailRecordRID)
            prevTailRecordPhysicalPages[SCHEMA_ENCODING_COLUMN].replaceRecord(locPhyPageIndex, 1)

        # Step 5: update base page with location of new tail record
        locType, locPRIndex, locBPIndex, locPhyPageIndex = self.page_directory.getRecordLocation(RID)
        basePagePhysicalPages = self.page_directory.getPhysicalPages(locType, locPRIndex, locBPIndex,
                                                                     locPhyPageIndex).physicalPages
        basePagePhysicalPages[INDIRECTION_COLUMN].replaceRecord(locPhyPageIndex, tailRecordRID)
        basePagePhysicalPages[SCHEMA_ENCODING_COLUMN].replaceRecord(locPhyPageIndex, 1)

    def getUpdatedRow(self, currValues, update):
        """
        Construct a new list of values that hold the most up to date version

        :param currValues: list. the oldest version of the data
        :param update: list. contains the values to be updated in currValues.
                             An entry that is None means it will not be updated.
        """
        updatedValues = []

        for i in range(len(currValues)):
            if update[i] is None:
                updatedValues.append(currValues[i])
            else:
                updatedValues.append(update[i])

        return updatedValues

    def getNewTimeStamp(self):
        """ Get time since epoch as integer"""
        return int(time())
