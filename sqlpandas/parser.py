"""
SQL for pandas.
"""

import pandas as pd
import logging
from collections import OrderedDict


class PandasSql:
    # valid_formats = {}

    valid_keywords = ["select", "from", "where", "group by", "order by"]

    def __init__(self):
        """

        """
        self._logger()
        self.df = None

    def _logger(self):
        """
        Logger.
        :return:
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def add_df(self,
               df):
        """

        :param df:
        :return:
        """
        self.df = df

    def read_file(self,
                  source_file,
                  source_type='csv',
                  **kwargs
                  ):
        """

        :param source_file:
        :param source_type:
        :param kwargs:
        :return:
        """
        format_function_mapping = {"csv": pd.read_csv,
                                   "json": pd.read_json}
        if source_type in format_function_mapping:
            self.df = format_function_mapping[source_type](source_file, **kwargs)
        else:
            self.logger.error(source_type, " is not a valid source type for sqlpandas.")
            self.logger.error("It may be added in later versions.")
            self.logger.error("please read dataframe using pandas and use sqlpandas.add_df instead")

    def parse_sql(self,
                  query):
        """

        :param query:
        :return:
        """
        pairs = self._get_pairs_string(query)
        spec = PandasSql.get_oriented_dict_tags_str(query, pairs)
        return spec

    def _get_pairs_string(self, full_string):
        """
        getting pairs of starting and ending position for tags in a certain string

        :param full_string:
        :return:
        """
        if len(full_string) > 0:
            _idx = {i: full_string.index(i) for i in self.valid_keywords if full_string.find(i) != -1}
            _idx = OrderedDict(sorted(_idx.items(), key=lambda k: k[1]))
            return OrderedDict(zip([i for i in _idx.keys()],
                                   [(list(_idx.values())[i], list(_idx.values())[i + 1]) if i < len(_idx.keys()) - 1
                                    else (list(_idx.values())[i],)
                                    for i in range(len(_idx.values()))]))
        else:
            return {i: (-1,) for i in self.valid_keywords}

    @staticmethod
    def get_oriented_dict_tags_str(full_content,
                                   pairs):
        """
        giving the read dictionary on position of keywords from string

        :param full_content:
        :param pairs:
        :return:
        """
        _oriented_list_tags = []
        for tag in pairs:
            if len(pairs[tag]) == 1:
                _oriented_list_tags.append(full_content[pairs[tag][0] + len(tag) + 1:])
            else:
                _oriented_list_tags.append(full_content[pairs[tag][0] + len(tag) + 1:pairs[tag][1]])
        _oriented_list_tags = [i.strip() for i in _oriented_list_tags]
        return dict(zip(list(pairs.keys()), _oriented_list_tags))

    def derived_column(self, entity_map):
        """

        :param entity_map:
        :return:
        """
        base_cols = self.df.columns
        _derivations = [col.split(" as ")
                        for col in entity_map["selection_columns"]
                        if " as " in col]
        entity_map["selection_columns"] = [col
                                           for col in entity_map["selection_columns"]
                                           if " as " not in col]
        if len(_derivations) > 0:
            _renames = dict([tuple(var) for var in _derivations if var[0] in base_cols])
            entity_map["renames"] = _renames
            if len(_renames) != len(_derivations):
                entity_map["derivations"] = [tuple(var)
                                             for var in _derivations
                                             if var[0] not in base_cols]

        if "renames" in entity_map:
            entity_map["selection_columns"].extend(list(entity_map["renames"].keys()))
        return entity_map

    @staticmethod
    def rename(df, entity_map):
        """

        :param df:
        :param entity_map:
        :return:
        """
        if "renames" in entity_map:
            df = df.rename(columns=entity_map["renames"])
        else:
            df = df

        return df

    def derivations(self, entity_map):
        """

        :param entity_map:
        :return:
        """
        if "derivations" in entity_map:
            self.logger.warning("following expressions are not supported still - ")
            self.logger.warning(*entity_map["derivations"])

    def entity_mapping(self, query):
        """

        :param query:
        :return:
        """
        entity_map = self.parse_sql(query)
        return_dict = {"df": "",
                       "selection_columns": [],
                       "where": "",
                       "order by": [],
                       "group by": []}

        if self.df is None:
            return_dict["df"] = locals()[entity_map["from"]]
        else:
            return_dict["df"] = self.df

        if entity_map["select"] == "*":
            return_dict["selection_columns"] = return_dict["df"].columns
        else:
            return_dict["selection_columns"] = [entry.strip() for entry in entity_map["select"].split(",")]

        if "where" in entity_map:
            return_dict["where"] = entity_map["where"]
        else:
            return_dict["where"] = ""

        if "order by" in entity_map:
            return_dict["order by"] = entity_map["order by"]

        if "group by" in entity_map:
            return_dict["group by"] = entity_map["group by"]

        return_dict = self.derived_column(return_dict)
        return return_dict

    def sql(self, query):
        """

        :param query:
        :return:
        """
        mapping = self.entity_mapping(query)
        valid_set = {key: mapping[key] for key in mapping if len(mapping[key]) > 0}

        df = valid_set["df"]

        if "where" in valid_set:
            df = df.query(valid_set["where"])

        if 'order by' in valid_set:
            if len(valid_set["order by"]) == 0:
                df = df.sort_index()
            else:
                df = df.sort_values(valid_set["order by"])

        df = df[valid_set["selection_columns"]]
        if "renames" in valid_set:
            df = self.rename(df, valid_set)

        if "derivations" in valid_set:
            self.derivations(mapping)

        return df


if __name__ == '__main__':
    sql = PandasSql()
    df1 = pd.read_csv("C:\\Users\\puneet\\PycharmProjects\\sqlpandas\\sample_files\\test.txt")
    sql.add_df(df1)
    dta = sql.sql("select a, c, d as ss from df order by d")
    print(dta)
