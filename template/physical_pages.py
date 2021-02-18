from template.config import *
from template.page import Page

#has a physical page for every column in the table
class PhysicalPages:
    def __init__(self, num_columns):
        self.physicalPages = []
        self.numOfRecords = 0

        for _ in range(num_columns):
            self.physicalPages.append(Page())

    # record location = [recordType, locPRIndex, locBPIndex or locTPIndex]
    #returns the RID of the newly created Record
    def setPageRecord(self, record, recordLocation):
        #set last item of recordLocation
        locPhyPageIndex = self.numOfRecords
        recordLocation.append(locPhyPageIndex)

        #create New RID with record Location
        RID = record.getNewRID(recordLocation[0], recordLocation[1], recordLocation[2], recordLocation[3])

        record.RID = RID

        self.physicalPages[INDIRECTION_COLUMN].write(record.indirection)
        self.physicalPages[RID_COLUMN].write(RID)
        self.physicalPages[TIMESTAMP_COLUMN].write(record.timestamp)
        self.physicalPages[SCHEMA_ENCODING_COLUMN].write(record.encoding)

        for col in range(RECORD_COLUMN_OFFSET, RECORD_COLUMN_OFFSET + len(record.columns)):
            columnData = record.columns[col - RECORD_COLUMN_OFFSET]

            self.physicalPages[col].write(columnData)

        self.numOfRecords += 1

        return RID

    def hasCapacity(self):
        if self.numOfRecords >= PAGE_SIZE:
            #page is full
            return False
        else:
            #there is still room to add at least 1 more record
            return True
