from template.config import *

#can store 4000kb
#4 bytes is allocated for each record
#1000 max records per page
class Page:

    def __init__(self):
        self.num_records = 0 #max: 1000
        self.data = bytearray()
    
    #if num_records < 1000 return true
    #if num_records >= 1000 return false
    def has_capacity(self):
        if self.num_records >= PAGE_SIZE:
            return False
        else:
            return True

    #input: integer
    #output: if page still has room return true, else return false
    #adds an integer to a page 
    def write(self, value):
        if self.has_capacity:
            byteArray = Page().integerToBytes(value)

            self.data += byteArray
            self.num_records += 1  

            return True #Successfully added record to page
        else:
            return False #Failed to add record to page 

    #input: int index from 0 - 1023999
    #output: integer value of that record
    def getRecord(self, index):
        x1, x2 = Page().getRecordIndexes(index)
    
        try:
            return Page().hexToInt(Page().bytesToHex(self.data[x1: x2]))
        except ValueError:
            return False

    #input: the index of the record to replace, new integer value
    #output: void
    def replaceRecord(self, index, newIntVal):
        x1, x2 = Page().getRecordIndexes(index)
        byteArray = Page().integerToBytes(newIntVal)

        try:
            self.data[x1: x2] = byteArray
            return True
        except IndexError:
            return False

    #input: record index to remove,
    #output: return true if removal was successful, false otherwise
    def removeRecord(self, index):
        x1, x2 = Page().getRecordIndexes(index)
        
        #TODO: del doesnt throw index error, need to remove outer if block
        if self.getRecord(index) != False:
            try:
                del self.data[x1: x2]
                self.num_records -= 1

                return True
            except IndexError:
                return False
        else:
            return False
        
    #input: index from 0 - 999
    #output: a list [x1,x2] (inclusive,exclusive)
    #indexes will allow you to retreive a slice from self.data
    @staticmethod
    def getRecordIndexes(index):
        return [index * RECORD_COLUMN_OFFSET, (index+1) * RECORD_COLUMN_OFFSET]

    #input: an interger -> must convert to 4 bytes, make sure its not too long
    #output: the hex(integer) in a bytearray format
    #hexed version of integer must be 8 bit?
    @staticmethod
    def integerToBytes(integer):
        return (integer).to_bytes(4,'big')

    #input:bytearray
    #output: Hex
    @staticmethod
    def bytesToHex(bytesArray):
        return bytesArray.hex()
    
    #input a hex string
    #output: integer
    @staticmethod
    def hexToInt(hexStr):
        return int(hexStr, 16)