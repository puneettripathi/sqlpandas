import os
import pandas as pd
from sqlpandas.parser import PandasSql
import unittest
from collections import OrderedDict

class TestPandasSql(unittest.TestCase):
    """

    """
    def test_add_df(self):
        df = pd.DataFrame({"a": [1, 2, 3],
                           "b": [23, 45, 67],
                           "c": [100, 200, 300],
                           "d": ['a', 'b', 'c']})
        sql = PandasSql()
        sql.add_df(df)
        self.assertEqual(all(sql.df == df), True)

    def test_read_file(self):
        df = pd.read_csv("test_files" + os.sep + "test.txt")
        sql = PandasSql()
        sql.read_file("test_files" + os.sep + "test.txt")
        self.assertEqual(all(sql.df == df), True)

    def test_parse_sql(self):
        _actual = {'select': '*', 'from': 'df'}
        sql = PandasSql()
        _query_spec = sql.parse_sql("select * from df")
        self.assertEqual(_query_spec, _actual)

    def test_get_pairs_string(self):
        _actual = OrderedDict([('select', (0, 13)), ('from', (13,))])
        sql = PandasSql()
        self.assertEqual(sql._get_pairs_string("select a,b,c from df"),
                         _actual)

    def test_get_oriented_dict_tags_str(self):
        _actual = {'select': 'a,b,c', 'from': 'df'}
        sql = PandasSql()
        _query = "select a,b,c from df"
        self.assertEqual(PandasSql.get_oriented_dict_tags_str(_query,
                                                              sql._get_pairs_string(_query)),
                         _actual)

    def test_derived_column(self):
        df = pd.DataFrame({"a": [1, 2, 3],
                           "b": [23, 45, 67],
                           "c": [100, 200, 300],
                           "d": ['a', 'b', 'c']
                           })
        sql = PandasSql()
        sql.add_df(df)
        self.assertEqual(sql.entity_mapping("select a, b as ff from df")['renames'], {'b': 'ff'})


if __name__ == '__main__':
    unittest.main()
