import sqlite3

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
cursor.execute("select count(1) from NATION ") #SELECT name FROM sqlite_master WHERE type='table';
#select * from nation
print(cursor.fetchall())