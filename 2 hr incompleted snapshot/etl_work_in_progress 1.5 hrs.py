import sqlite3
import logging
import csv
import os
import data_validation

# Dict for Number of fields in each table
dic = {

        "NATION":4
        ,"REGION":3
        ,"PART":9
        ,"SUPPLIER":7
        ,"PARTSUPP":5
        ,"CUSTOMER":8
        ,"ORDERS":9
        ,"LINEITEM":16
}

# Because table name is same as file name,
'''dic_table_file = {

        "NATION":"nation.tbl"
       
}'''


def init_conn(db):
	con = None
		
	try:
			con = sqlite3.connect( db )
			
		   
	except sqlite3.Error as e:
			logging.Exception("Database error: %s" % e) 
	except Exception as e:
			logging.Exception("Exception in _query: %s" % e)
	return con

def run_script(filename):
	

	conn = init_conn('example.db')
	cursor = conn.cursor()
	
	with open(filename, 'r') as file:
		script_content = file.read()

		result = cursor.executescript(script_content)



	if conn:
				conn.close()
	
def read_files(rootdir,file_extension,delimit_char,broken_list):



	messages=[]
	for folder, subs, files in os.walk(rootdir):

		full_file=os.path.join(folder, filename)
		for filename in files :
			# for 
			
			if file_extension in full_file and full_file not in broken_list:
					
				reader = csv.reader(open(filename), delimiter=delimit_char)
	
				for row in reader:
					messages.append(row)
					print(i, "rows from", import_file, "uploaded into database")

bad_files=get_files("C:\PersonalProj\interviews\interview-test-data-engineer\data",".tbl","|")[1]

run_script("ddl.sql")
read_files()

# bulk insert the messages
messages = [(msg.id, msg.string) for msg in cat]
result = cursor.executemany('INSERT INTO ru  VALUES (?, ?)',  messages)
assert(result.rowcount == len(messages))
conn.commit()


