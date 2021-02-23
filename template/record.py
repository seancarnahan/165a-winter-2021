class Record:

    #input: columns = list of integers
    def __init__(self, key, indirection, timestamp, encoding, columns):
        #not sure
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

        self.base_RID = None

    #input: record location
    #output: record location in integer form; #example: 123456789
    def getNewRID(self, recordType, locPRIndex, locBPIndex, locPhyPageIndex):
        num = recordType*(10**8) + locPRIndex*(10**6) + locBPIndex*(10**4) + locPhyPageIndex

        return num
