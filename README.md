# How things work

## Page Directory

The Page Directory maps a record RID to the page it resides in.

- This class holds the page ranges for the given table.

### Setup:
- sudo pip install colorama (make sure this package is on your machine)

### Notes

- The Page Directory Class holds the page ranges, not the table class.
-
-

## Page Range

The Page Range encompasses the base pages and tails pages of a table.

At first we create a set of base pages (let's say 16) and a single tail page for each column.

![Page Range Img](images/pageRange1.png)

"Each base page (or tail page) consists of a set of physical pages (4 KB each), <b>one for each column</b>."

### Notes

- Starts with only 1 tail page at first. Add more as you fill them up
- Records are spread out over these pages.
-

## Record

### Attributes

- RID: Record ID
- Indirection column: Stores the RID of the latest updated record. None if it is a base record and no updates have been done. If a tail record, points to the previous tail record
- Schema Encoding Column: a bitmap (bit vector) with size equal to the number of columns. Tells us if a column has been updated. 0 if not updated, 1 if updated.
- Timestamp Column: the time at which it was created.

### Notes

- The schema encoding column in a tail record is useless for us since we are doing the cumulative approach to tail records.
- A record can be a base record or a tail record depending if it is stored in a base page or a tail page.
-

## Base Page

- Read-Only
- Only add record to base page if it a new record
- Consists of a set of physical pages, one for each column of the table.

### Notes

-
-
-

## Tail Page

- Append-Only
- Using cumulative approach so each new tail record will hold the latest values for each column so we don't need to traverse the lineage.

### Notes

-
-
-

### how updateRecord in Table works:
      
- Given an RID of the record you would like to update
    - 1. first add the tail page to the correct Page Range and return the RID of the new tail page
    - 2. next go into the base page and change encoding to 1, and then change indirection to the new RID
    - note its up to the query to get the most updated values and combine them with the new values and the call updateRecord()

## Data Structure
-          PageRanges [
                pageRange: 0: 0 - 5000 records #cutoff: 5000 records 
                    [physicalpage for col0, page for col1, page for col2], => base page 0 : 1000 records #4kb * cols
                    [physicalpage for col0, page for col1, page for col2], => base page 1 : 1000 records #4kb * cols
                    [physicalpage for col0, page for col1, page for col2], => base page 2 : 1000 records #4kb * cols
                    [physicalpage for col0, page for col1, page for col2], => base page 3 : 1000 records #4kb * cols
                    [physicalpage for col0, page for col1, page for col2], => base page 4 : 1000 records #4kb * cols
                pageRange: 1: 5001 - 10000 records
            ]
