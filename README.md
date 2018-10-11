# SQL For Pandas
##### The library can take any `pandas dataframe` and you should be able to run `SQL` on top of `dataframe` then. `pandas` already provides a nice way of working with dataset. `sqlpandas` is an initiative to bring `pandas` to `SQL` folks too.

##### Support for file formats -
* CSV
* JSON
* Other files must first be read using `pandas` and then should be added to `sqlpandas` instance

##### Examples -
1. Example for adding dataset to sql object
```python
import pandas as pd
from sqlpandas.parser import PandasSql
sql = PandasSql()
df1 = pd.read_csv("C:\\Users\\puneet\\PycharmProjects\\sqlpandas\\sample_files\\test.txt")
sql.add_df(df1)
dta = sql.sql("select * from df where b > 3 order by d")
```

2. Examaple for reading CSV and then querying

```python
import pandas as pd
from sqlpandas.parser import PandasSql
sql = PandasSql()
sql.read_file("C:\\Users\\puneet\\PycharmProjects\\sqlpandas\\sample_files\\test.txt")
dta = sql.sql("select * from df where b > 3 order by d")
```

##### For any questions or if you want to contribute contact 
###### Puneet Tripathi
###### Email: puneet.tripathim@gmail.com
###### LinkedIn: https://www.linkedin.com/in/puneet-tripathi/
