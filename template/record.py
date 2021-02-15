class Record:
    """
    A Record Object stores metadata regarding an entry in a table (row).

    Attributes:
        key = the primary key of the record
        indirection = the RID of the latest updated version of the record. 0 if it is a base record
        timestamp = the time at which this record what added
        encoding = a value which tells you whether an update exists for this record.
                    0 = no update. 1 = tail record exists. 2 = deleted/invalid record
        columns = a list of data values. This corresponds to the columns of the table it belongs to.
        RID = the rid of this record.
    """

    def __init__(self, key, indirection, timestamp, encoding, columns):

        self.key = key

        #an RID if this is an update
        #set to 0 if this is the BASE record
        self.indirection = indirection

        #integer (based on milliseconds from epoch)
        self.timestamp = timestamp

        #bitmap
        self.encoding = encoding

        #list corresponding to all of the user created columns
        self.columns = columns

        #must call getNewRID for this to be added
        self.RID = None

    #input: record location
    #output: record location in integer form; #example: 123456789
    def getNewRID(self, locType, locPRIndex, locBPIndex, locPhyPageIndex):
        """ Create a new RID. """
        num = locType*(10**8) + locPRIndex*(10**6) + locBPIndex*(10**4) + locPhyPageIndex

        return num