import sqlite3

from etl_main import *

output_db="example.db"

def init_conn(db):
	con = None
		
	try:
			con = sqlite3.connect( db )
			
		   
	except sqlite3.Error as e:
			logging.Exception("Database error: %s" % e) 
	except Exception as e:
			logging.Exception("Exception in _query: %s" % e)
	return con

def run_script(cursor):
	


	
	try:
		

			result = cursor.execute("select count(1) from part")
			rows = cursor.fetchall()
			for row in rows:
				print(row)
 

	except sqlite3.Error as e:
			logging.exception("Database error: %s" % e) 

conn = init_conn(output_db)
cursor = conn.cursor()

run_script(cursor)

con = sqlite3.connect('example.db')
cursor = con.cursor()
#cursor.execute("select c1.*, case when c1.C_acctbal<lowerQuartile then 'low' when c1.C_acctbal<upperQuartile then 'mid' else 'high' end as bal_group from customer c1 cross join (select min(C_acctbal),max(C_acctbal),((MIN(C_acctbal)+ AVG(C_acctbal)) / 2) AS lowerQuartile,((MAX(C_acctbal)+ AVG(C_acctbal)) / 2) AS upperQuartile from customer ) c2 ") #SELECT name FROM sqlite_master WHERE type='table';
#select * from nation
#print(cursor.fetchall())

with open("top 5 nation by rev.SQL", 'r') as file:
			script_content = file.read()
			cursor.execute(script_content)
			print(cursor.fetchall())

