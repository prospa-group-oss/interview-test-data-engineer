## How to use (2 production scripts)
1. data_validation.py can be used to evaluate a) whether there are rows with extra or missing columns; b) number of records
2. Just run etl_main.py to load the file
3. select_result.py can be used to test records have been written

## file to review for interviewer:
data_validation.py

etl_main.py

select_result.py

Answers:

1. File format is CSV( pipe delimited)

2. See .py files

3  Would use a tool to deal with slow change dimension etc for star schema, may try [pygrametl](http://chrthomsen.github.io/pygrametl)

4.1 done in : bonus answer.sql

5: will use Apache airflow or Glue ETL scheduler, if not available then a Windows scheduled task or Linux cron

6. Test before deploy to scheduling tool
Yes can setup a docker

## Data Reporting.

1 done as an example: top 5 nation by rev.SQL  

(answer: ('CANADA', 3545033470.4100056), ('EGYPT', 3465584296.767509), ('IRAN', 3376856530.8189025), ('ALGERIA', 3154835599.520595), ('BRAZIL', 3129727100.2945952),


## Caveat

For all files, there is an extra separator at the end of each line, therefore we had to remove the last column on the fly before write to DB.




## Consideration of license-free product, either MYSQL or SQLite, use SQLite as encoded instruction


## pre-requisite: SQLite needs to be installed
## Time used:
35 min on data_validation.py, ~ 100 minutes on ETL_main.py, 30 min to debug

5 min on extra bonus question (divide customer Balance to 3 groups, used 25% 75% quartiles)


## base64 bonus instruction:
Use the instruction on https://github.com/lovasoa/TPCH-sqlite to generate your data files.
The data.zip file were generated with scale factor of 0.01
Please, encode your file with the instruction you used to generate the data files.


