## How to use (2 production scripts)
1. data_validation.py can be used to evaluate a) whether there are rows with extra or missing columns; b) number of records
2. Just run etl_main.py to load the file
3. select_result.py can be used to test records have been written

## Caveat

For all files, there is an extra separator at the end of each line, therefore we had to remove the last column on the fly before write to DB.




## Consideration of license-free product, either MYSQL or SQLite, use SQLite as encoded instruction


## pre-requisite: SQLite needs to be installed
## Time used:
35 min on data_validation.py, ~ 100 minutes on ETL_main.py, 30 min to debug


## base64 bonus instruction:
Use the instruction on https://github.com/lovasoa/TPCH-sqlite to generate your data files.
The data.zip file were generated with scale factor of 0.01
Please, encode your file with the instruction you used to generate the data files.


