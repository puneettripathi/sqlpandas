import pandas as pd
from sqlpandas.parser import PandasSql


sql = PandasSql()
# df1 = pd.read_csv("C:\\Users\\puneet\\PycharmProjects\\sqlpandas\\sample_files\\test.txt")
# sql.add_df(df1)
sql.read_file("C:\\Users\\puneet\\PycharmProjects\\sqlpandas\\sample_files\\test.txt")
dta = sql.sql("select a, c, d as ss from df order by d")
print(dta)
