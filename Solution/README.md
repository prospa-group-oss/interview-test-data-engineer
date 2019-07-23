# Data Engineer Interview Test

This fork is in response to the [data engineer interview test] for Prospa.

# Solution
## Overview
This solution takes the supplied set of tabular data files and loads the data into a raw database in SQLite3. The data then undergoes some simple ETL to load a star schema data warehouse from which some reporting questions are answered.

## Tools and Technologies
The following tools and technologies are used to deliver the solution:
* SQLite3 – houses both the raw and data warehouse databases.
* Python 3.6 – reads the raw source files and loads the data into the raw database and then loads the DWH star schema from raw data.

## Instructions
### Pre-requisites
Please ensure the following python modules are installed:
* SQLite3 – version must be 3.25.0 or above ([upgrade DDLs])
* pandas 

### Getting Started
Download and unzip the solution.zip folder. For the sake of this document, it is assumed the unzipped folder is in the c:\\.

Open command prompt and navigate to this unzipped folder.
```sh
cd c:\solution`
```

### Environment setup
From here you will run the `setup.py` file to create the a folder to house the database files and generate the tables in both the raw and dwh databases.
```sh
python setup.py
```

### Import raw data & populate the Data Warehouse
Run the following to populate both the raw and dwh databases:
```sh
python loadData.py
```

### Query the Data Warehouse
There is a module which can be run to generate the answers to reporting questions below.
```sh
python reporting.py
``` 

## Questions
### The small ETL project
1. The data for this exercise can be found on the `data.zip` file. Can you describe the file format?
The data files are tabular pipe delimitered files (with no text qualifiers or header rows).

**Super Bonus**: generate your own data through the instructions on the encoded file `bonus_etl_data_gen.txt`.
To get the bonus points, please encoded the file with the instructions were used to generate the files.
Doesn't count, but please see `generatedData.txt`

2. Code your scripts to load the data into a database.
These can be found in `loadData.py`. Uses a replace table methodlogy. 

3. Design a star schema model which the data should flow.
TODO

4. Build your process to load the data into the star schema 
These can be found in `loadData.py`. Uses a replace table methodlogy. For simplicity sake, I did not create surrogate keys or a date dimension table.

**Bonus** point: 
- add a fields to classify the customer account balance in 3 groups 
Added tercile categories LOW/MEDIUM/HIGH
- add revenue per line item 
Calculation: l_extendedprice * (1 - l_discount)
Caveat: since l_discount values are all zero in the dataset, the revenue will be equal to the extended price
- convert the dates to be distributed over the last 2 years
I'm not sure what is meant by this. Will clarify.

5. How to schedule this process to run multiple times per day?
There are several scheduling tools to choose from, e.g. Airflow, Oozie or operating system schedulers.
 
**Bonus**: What to do if the data arrives in random order and times via streaming?
Firstly, I would not use this batch process to load the streaming data. Would look to implement a cloud based serverless framework to stream data into a databse. Would have to build in logic to cater for out of order data.

6. How to deploy this code?
Github pipeline, Docker

**Bonus**: Can you make it to run on a container like process (Docker)? 
Have never used Docker, but will be experimenting


## Data Reporting
One of the most important aspects to build a DWH is to deliver insights to end-users. Besides the question bellow, what extra insights you can think of can be generated from this dataset?
Some additional insights that could be generated:
* cheapest supplier for a part
* profit margin
* annual profit/loss
* lost revenue from returned orders
* ability to fill orders (based on orders to fill and parts available from supplier)
* market share
* profit by product type
* growth opportunities in low revenue nations

Can you using the designed star schema (or if you prefer the raw data), generate SQL statements to answer the following questions:
1. What are the top 5 nations in terms of revenue?

Code:
```sh
SELECT 
    c.c_nation, 
    SUM(lo.lo_revenue) AS totalrevenue 
FROM 
    lineorder lo 
    INNER JOIN customer c ON c.c_custkey = lo.lo_custkey 
WHERE
    lo.lo_returnflag <> 'R'
GROUP BY c.c_nation 
ORDER BY 2 
DESC LIMIT 5;
```
Answer:
| Nation |
| ------ |
|CANADA|
|EGYPT|
|IRAN|
|ALGERIA|
|BRAZIL|

2. From the top 5 nations, what is the most common shipping mode?
  lo_shipmode

Code:
```sh
WITH nation AS (
    SELECT 
        c.c_nation, 
        SUM(lo.lo_revenue) AS totalrevenue 
    FROM 
        lineorder lo 
        INNER JOIN customer c ON c.c_custkey = lo.lo_custkey 
    WHERE\
        lo.lo_returnflag <> 'R'
    GROUP BY c.c_nation 
    ORDER BY 2 
    DESC LIMIT 5
)
SELECT 
    lo.lo_shipmode,
    COUNT(1) AS shipmodecount
FROM
    nation n
    INNER JOIN customer c ON c.c_nation = n.c_nation
    INNER JOIN lineorder lo ON c.c_custkey = lo.lo_custkey
GROUP BY lo.lo_shipmode
ORDER BY 2 desc
LIMIT 1;
```
Answer:
| Ship Mode |
| ------ |
|FOB|

3. What are the top selling months?

Code: (limited to 3)
```sh
SELECT
    STRFTIME('%m', lo.lo_orderdate) AS Month,
    SUM(lo.lo_revenue) AS totalrevenue
FROM
    lineorder lo
WHERE
    lo.lo_returnflag <> 'R'
GROUP BY STRFTIME('%m', lo.lo_orderdate)
ORDER BY 2 DESC
LIMIT 3;
```
Answer:
| Month |
| ------ |
|May|
|July|
|June|

4. Who are the top customer in terms of revenue and/or quantity? 
 
Revenue:
Code:
```sh
SELECT 
    c.c_custkey,
    c.c_name,
    SUM(lo.lo_revenue) AS totalrevenue
FROM
    lineorder lo
    INNER JOIN customer c ON c.c_custkey = lo.lo_custkey
WHERE
    lo.lo_returnflag <> 'R'
GROUP BY
    c.c_custkey,
    c.c_name
ORDER BY 2 DESC
LIMIT 5
```
Answer:
| Customer Name |
| ------ |
|Customer#000001499|
|Customer#000001498|
|Customer#000001496|
|Customer#000001495|
|Customer#000001493|

Quantity:
Code:
```sh
SELECT 
    c.c_custkey,
    c.c_name,
    SUM(lo.lo_quantity) AS totalquantity
FROM
    lineorder lo
    INNER JOIN customer c ON c.c_custkey = lo.lo_custkey
WHERE
    lo.lo_returnflag <> 'R'
GROUP BY
    c.c_custkey,
    c.c_name
ORDER BY 2 DESC
LIMIT 5;
```
Answer:
| Customer Name |
| ------ |
|Customer#000001499|
|Customer#000001498|
|Customer#000001496|
|Customer#000001495|
|Customer#000001493|

5. Compare the sales revenue of one current period against previous period?

Code:
```sh
WITH yearRevenue AS (
    SELECT
        STRFTIME('%Y', lo_orderdate) AS yr,
        SUM(lo_revenue) AS revenue,
        LAG(SUM(lo_revenue)) OVER(ORDER BY STRFTIME('%Y', lo_orderdate) ASC) AS previousyearrevenue
    FROM
        lineorder lo
    WHERE
        lo.lo_returnflag <> 'R'
    GROUP BY STRFTIME('%Y', lo_orderdate)	
)
SELECT
    y.yr,
    y.revenue,
    y.previousyearrevenue,
    ROUND((y.revenue - y.previousyearrevenue) / y.previousyearrevenue * 100, 2) AS yoypercent
FROM 
    yearRevenue y;
```
Answer:
| Year |Revenue|Previous Revenue|YOY %|
| ------ | ------ | ------ | ------ |
|1992|1.529228e+08|||
|1993|1.606991e+08|1.529228e+08|5.09|
|1994|1.555311e+08|1.606991e+08|-3.22|
|1995|2.680593e+08|1.555311e+08|72.35|
|1996|3.119284e+08|2.680593e+08|16.37|
|1997|3.079926e+08|3.119284e+08|-1.26|
|1998|1.800052e+08|3.079926e+08|-41.56|

## Data profilling 
Data profiling are bonus.

What tools or techniques you would use to profile the data?
Many third party tools are available for profiling data sets, for example Aggregate Profiler or Talend Open Studio. While these tools can do a lot of the data profiling tasks, I still think getting down in the detail and analysing the data yourself offers great insight and increases understanding of the datasets.
 
What results of the data profiling can impact on your analysis and design? 
* orphans - there may be missing referential data
* business rules - can identify business rules required for ETL processes  
* useless data - can identitify if data is no longer/has ever been used, i.e. columns where data vlues are ALWAYS NULL
* data quality - ongoing data profiling can assist with pro-active data quality


## Architecture
If this pipeline is to be build for a real live environment.
What would be your recommendations in terms of tools and process?
I would look to building a serverless cloud based streaming solution.

Would be a problem if the data from the source system is growing at 6.1-12.7# rate a month?
Should not be an issue with appropriate scaling policies in cloud based architectures.

**References**
http://www.sqlitetutorial.net/sqlite-python/
https://www.dataquest.io/blog/python-pandas-databases/

[data engineer interview test]: <https://github.com/prospa-group-oss/interview-test-data-engineer>
[upgrade DDLs]: <https://www.sqlite.org/download.html>



