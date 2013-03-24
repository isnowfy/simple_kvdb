# -*- coding: utf-8 -*-

import unittest

import simple_kvdb as skvdb

class SkvdbTest(unittest.TestCase):
    def test_other(self):
        self.assertRaises(skvdb.NotSupportedException, skvdb.get_skvdb, 'oo')

if __name__ == '__main__':
    unittest.main()
