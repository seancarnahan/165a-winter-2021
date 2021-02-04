import unittest
from template.table import Table, Record
from template.index import Index
from template.config import *

class TestQueryFunctionality(unittest.TestCase):
   def setUp(self):
      self.name = "grades"
      self.key = 1234
      self.num_columns = 5

      self.table = Table(self.name, self.num_columns, self.key)

