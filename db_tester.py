from template.db import Database
import unittest


class TestDBMethods(unittest.TestCase):

    def test_create_table(self):
        db = Database()
        my_table = db.create_table('Grades', 0, 5)
        self.assertIn(my_table, db.tables)

    def test_drop_table(self):
        db = Database()
        drop_table = db.create_table('dropme', 0, 5)
        db.drop_table(drop_table.name)
        self.assertNotIn(drop_table, db.tables)

    def test_get(self):
        db = Database()
        get_table = db.create_table('get', 0, 5)
        db.get_table(get_table.name)
        self.assertTrue(db)


if __name__ == '__main__':
    unittest.main()
