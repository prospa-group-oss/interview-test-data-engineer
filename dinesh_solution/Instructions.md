# Instruction to Execute and Answers to questions

1. Have used Mysql as a database to load the data and create datawareshouse

2. was able to decode the base64 encoded instruction file and generated my own data using scale factor of 0.2

First step is to run TPCH-sqlite with scale factor of 0.1 and place the files in dinesh_solution/landing/ folder

3. Air flow can be used to orchestrate these scripts

4. Project has be categorised into 5 different scripts
   4.1 initial_load.py - python script to load the .tbl files into Mysql table(Note : please create retail as name of the database and have all the SQL's of create tables executed prior to running this script)
   4.2 date_dimension_load.py - python script to create date dimension table and load dates
   4.3 customer_dimension_load.py - python script to extract, transform and load customer retaled attributes to CUSTOMERDIM
   4.4 part_supplier_dimension_load.py - python script to extract, transform and load part and supplier attribiutes to PARTSUPPDIM
   4.5 lineitem_fact_load.py - python script to extract, transform and load lineitem attributes to LINEITEMFACT

   Please note: All these five scripts are developed as standalone with main method, they can be executed straign away(However this can be made into frame work in a real life scenario)

The small ETL project
--------- 

1. The data for this exercise can be found on the `data.zip` file. Can you describe the file format?

**Super Bonus**: generate your own data through the instructions on the encoded file `bonus_etl_data_gen.txt`.
To get the bonus points, please encoded the file with the instructions were used to generate the files.

"Instructions were encoded in base64, I have used scale factor of 0.2: Use the instruction on https://github.com/lovasoa/TPCH-sqlite to generate your data files.  "

2. Code you scripts to load the data into a database. - Available as part of the instruction provided above

3. Design a star schema model which the data should flow. - Table schemas attached under folder /datawarehouse/

4. Build your process to load the data into the star schema  - Available as part of the instruction provided above


**Bonus** point: 
- add a fields to classify the customer account balance in 3 groups - Classied balance into three groups (refer to customer_dimension_load.py)
- add revenue per line item - able to generate revenue per line item(refer to lineitem_fact_load.py)
- convert the dates to be distributed over the last 2 years - Did not understand the question, if the intenstion was to test date manipulation , I have create date dimension with all the necessary date formats

5. How to schedule this process to run multiple times per day? - Airflow DAG can be used to schedule
 
**Bonus**: What to do if the data arrives in random order and times via streaming? (do dynamic partition of the stream data based on the update timestamp of the data itself.. can be discussed further...)

6. How to deploy this code? (pip ?)

**Bonus**: Can you make it to run on a container like process (Docker)? (Zero with Docker)

Data Reporting
-------
One of the most important aspects to build a DWH is to deliver insights to end-users. Besides the question bellow, what extra insights you can think of can be generated from this dataset?

Can you using the designed star schema (or if you prefer the raw data), generate SQL statements to answer the following questions:




1. What are the top 5 nations in terms of revenue?

mysql> SELECT C.N_NAME, SUM(L.L_QUANTITY * L.L_EXTENDEDPRICE) AS TOTAL_REVENUE FROM CUSTOMERDIM C, LINEITEMFACT L
    -> WHERE C.C_CUSTKEY = L.O_CUSTKEY
    -> GROUP BY C.N_NAME 
    -> ORDER BY TOTAL_REVENUE DESC
    -> ;
+----------------+---------------+
| N_NAME         | TOTAL_REVENUE |
+----------------+---------------+
| CHINA          |   61465375469 |
| EGYPT          |   60436019027 |
| IRAN           |   60421032047 |
| VIETNAM        |   60397239000 |
| IRAQ           |   60272614928 |
| UNITED STATES  |   60248822637 |


2. From the top 5 nations, what is the most common shipping mode?

mysql> SELECT  L.L_SHIPMODE, COUNT(*) FROM CUSTOMERDIM C, LINEITEMFACT L
    -> WHERE C.C_CUSTKEY = L.O_CUSTKEY
    -> AND C.N_NAME IN ('CHINA', 'EGYPT', 'IRAN', 'VIETNAM','IRAQ')
    -> GROUP BY L.L_SHIPMODE
    -> ;
+------------+----------+
| L_SHIPMODE | COUNT(*) |
+------------+----------+
| FOB        |    35273 |
| SHIP       |    35497 |
| MAIL       |    35236 |
| AIR        |    35914 |
| RAIL       |    35341 |
| TRUCK      |    35553 |
| REG AIR    |    35218 |
+------------+----------+


3. What are the top selling months?

mysql> SELECT D.month, SUM(L.L_EXTENDEDPRICE) AS REVENUE_BY_MONTH FROM DATES D , LINEITEMFACT L
    -> WHERE  L.O_ORDERDATE = D.DATE
    -> GROUP BY D.month
    -> ORDER BY REVENUE_BY_MONTH DESC
    -> ;
+-----------+------------------+
| month     | REVENUE_BY_MONTH |
+-----------+------------------+
| July      |       3932451735 |
| January   |       3924446503 |
| May       |       3919796282 |
| March     |       3887720452 |
| June      |       3804786067 |
| April     |       3793241191 |
| February  |       3639164563 |
| August    |       3402657522 |
| October   |       3358651422 |
| December  |       3351363248 |
| September |       3240257189 |
| November  |       3236289254 |
+-----------+------------------+

4. Who are the top customer in terms of revenue and/or quantity?

mysql> SELECT C.C_CUSTKEY, C.C_NAME, SUM(L.L_QUANTITY) AS QUANTITY FROM CUSTOMERDIM C, LINEITEMFACT L
    -> WHERE C.C_CUSTKEY = L.O_CUSTKEY
    -> GROUP BY C.C_CUSTKEY, C.C_NAME
    -> ORDER BY QUANTITY DESC LIMIT 10;
+-----------+--------------------+----------+
| C_CUSTKEY | C_NAME             | QUANTITY |
+-----------+--------------------+----------+
|       691 | Customer#000000691 |     4461 |
|     20959 | Customer#000020959 |     4186 |
|     25936 | Customer#000025936 |     4179 |
|      4351 | Customer#000004351 |     4136 |
|       979 | Customer#000000979 |     4134 |
|     26524 | Customer#000026524 |     4119 |
|     10348 | Customer#000010348 |     4075 |
|     14068 | Customer#000014068 |     4003 |
|     12532 | Customer#000012532 |     3933 |
|     20437 | Customer#000020437 |     3914 |

5. Compare the sales revenue of on current period against previous period?

Answers to the questions have been provided ABOVE. Apart from these, these are few extra insights that could be generated

0.1 Orders which has more than 7 days of difference b/w ship date and recipt date and their shipmode
mysql> SELECT L_SHIPMODE, COUNT(*) FROM LINEITEMFACT
    -> WHERE DATEDIFF(L_RECEIPTDATE, L_SHIPDATE) > 2
    -> GROUP BY L_SHIPMODE
    -> ;
+------------+----------+
| L_SHIPMODE | COUNT(*) |
+------------+----------+
| TRUCK      |   160277 |
| MAIL       |   159408 |
| AIR        |   160341 |
| RAIL       |   160172 |
| SHIP       |   160340 |
| FOB        |   159747 |
| REG AIR    |   159765 |
+------------+----------+

0.2 TOP 5 suppliers in terms of item quantities?
mysql> SELECT S.S_NAME, sum(L.L_QUANTITY) AS SUPPLIED_QTY from LINEITEMFACT L, PARTSUPPLIERDIM S
    -> WHERE L.L_SUPPKEY = S.PS_SUPPKEY
    -> GROUP BY S_NAME
    -> ORDER BY SUPPLIED_QTY DESC
    -> ;
0.3 Number of orders in each order status

  SELECT O_ORDERSTATUS, COUNT(*) AS ORDER_COUNT LINEITEMFACT 
  GROUP BY O_ORDERSTATUS
  ORDER BY ORDER_COUNT

0.4 customers who has returned more number of orders based on the L_RETURNFLAG?

  SELECT  C.C_NAME, COUNT(*) AS RETURN_COUNT FROM CUSTOMERDIM C, LINEITEMFACT L
  WHERE C.C_CUSTKEY = L.O_CUSTKEY
  AND RETURN_COUNT = 'Y'
  GROUP BY  C.C_NAME
  ORDER BY RETURN_COUNT DESC LIMIT 10;



Data profilling
----   
Data profiling are bonus.

What tools or techniques you would use to profile the data?
 
What results of the data profiling can impact on your analysis and design?   



Architecture
-----
If this pipeline is to be build for a real live environment.
What would be your recommendations in terms of tools and process?

Would be a problem if the data from the source system is growing at 6.1-12.7% rate a month?



ERD
--
![alt text](erd.png "ERD")

Author: adilsonmendonca
