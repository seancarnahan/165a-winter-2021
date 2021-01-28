from template.page import Page

#TODO: learn python unittesting framework, to make this more concise, im aware this looks awful lol
#TEST: page : has_capacity function
def test_has_capacity():
    page = Page()

    for i in range(999):
        page.write(i)

    if page.has_capacity() == True:
        pass
    else:
        print("Error in [test_has_capacity]: 1")
        return False

    page.num_records = 1000

    if page.has_capacity() == False:
        pass
    else:
        print("Error in [test_has_capacity]: 2")
        return False

    page.num_records = 999

    if page.has_capacity() == True:
        return True
    else:
        print("Error in [test_has_capacity]: 3")
        return False

#TEST: page : write function
def test_write():
    page = Page()

    for i in range(1000):
        page.write(i)

    if page.num_records == 1000:
        pass
    else:
        print("Error in [test_write]: 1")
        return False

    if len(page.data) == 4000:
        return True
    else:
        print("Error in [test_write]: 2")
        return False

#TEST: page : getRecord function
def test_getRecord():
    page = Page()

    for i in range(1000):
        page.write(i)
    
    if page.getRecord(1) == 1:
        pass
    else:
        print("Error in [test_getRecord]: 1")
        return False

    if page.getRecord(999) == 999:
        pass
    else:
        print("Error in [test_getRecord]: 2")
        return False

    if page.getRecord(1000) == False:
        return True
    else:
        print("Error in [test_getRecord]: 3")
        return False
    

#TEST: page : replaceRecord function
def test_replaceRecord():
    page = Page()

    for i in range(1000):
        page.write(i)

    if page.replaceRecord(2, 100000) == True:
        pass
    else:
        print("Error in [test_replaceRecord]: 1")
        return False

    if page.getRecord(2) == 100000:
        pass
    else:
        print("Error in [test_replaceRecord]: 2")
        return False

    if page.getRecord(1000) == False:
        return True
    else:
        print("Error in [test_replaceRecord]: 3")
        return False


#TEST: page : removeRecord function
def test_removeRecord():
    page = Page()

    for i in range(1000):
        page.write(i)
    
    if page.removeRecord(2) == True:
        pass
    else:
        print("Error in [test_removeRecord]: 1")
        return False

    if page.getRecord(2) == 3:
        pass
    else:
        print("Error in [test_removeRecord]: 2")
        return False

    if page.removeRecord(1000) == False:
        pass
    else:
        print("Error in [test_removeRecord]: 3")
        return False

    if page.num_records == 999:
        return True
    else:
        print("Error in [test_removeRecord]: 4")
        return False

#TEST: page : getRecordIndexes function
def test_getRecordIndexes():
    x1, x2 = Page().getRecordIndexes(3)

    if x1 == 12 and x2 == 16:
        return True
    else:
        print("Error in [test_getRecordIndexes]: 1")
        return False

#TEST: page : integerToBytes function
def test_integerToBytes():
    expectedVal = b'6\n\x87W'
    val = Page().integerToBytes(906659671)

    if val == expectedVal:
        return True
    else:
        print("Error in [test_integerToBytes]: 1")
        return False

#TEST: page : bytesToHex function
def test_bytesToHex():
    
    expectedVal = "360a8757"
    val = Page().bytesToHex(Page().integerToBytes(906659671))

    if val == expectedVal:
        return True
    else:
        print("Error in [test_bytesToHex]: 1")
        return False

#TEST: page : hexToInt function
def test_hexToInt():
    expectedVal = 906659671
    val = Page().hexToInt(Page().bytesToHex(Page().integerToBytes(906659671)))

    if val == expectedVal:
        return True
    else:
        print("Error in [test_hexToInt]: 1")
        return False

def main():
    if test_has_capacity() and test_write() and test_getRecord() and test_replaceRecord() and test_removeRecord() and test_getRecordIndexes() and test_integerToBytes() and test_bytesToHex() and test_hexToInt():
        print("ALL tests in [page_tester.py] pass successful") 
    else:
        print("There is at least 1 failing test")

if __name__ == "__main__":
    main()