# Global Setting for the Database
# PageSize, StartRID, etc..

def init():
    pass

#number of records that page can store
PAGE_SIZE = 1000

#MAX num of BASE PAGES per PAGE RANGE
MAX_PAGE_RANGE_SIZE = 20

# Table Columns
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4  # RID of base record

RECORD_COLUMN_OFFSET = 5
PAGE_RECORD_SIZE = 4

#Buffer POOL
# BUFFER_POOL_NUM_BASE_PAGES = 15
# BUFFER_POOL_NUM_TAIL_PAGES_PER_BASE = 2
BUFFER_POOL_NUM_OF_PRs = 3
