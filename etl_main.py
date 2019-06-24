import sqlite3
import logging
import csv
import os

from data_validation import * 

output_db='example.db'

# Dict for Number of fields in each table
dic = {

        "nation":(4,['N_NATIONKEY','N_NAME','N_REGIONKEY','N_COMMENT'])
        ,"region":(3,['R_REGIONKEY','R_NAME','R_COMMENT'])

        ,"part":(9,['P_PARTKEY','P_NAME','P_MFGR','P_BRAND','P_TYPE','P_SIZE','P_CONTAINER','P_RETAILPRICE','P_COMMENT'])
        ,"supplier":(7,['S_SUPPKEY','S_NAME','S_ADDRESS','S_NATIONKEY','S_PHONE','S_ACCTBAL','S_COMMENT'])
        ,"partsupp":(5,['PS_PARTKEY','PS_SUPPKEY','PS_AVAILQTY','PS_SUPPLYCOST','PS_COMMENT'])
        ,"customer":(8,['C_CUSTKEY','C_NAME','C_ADDRESS','C_NATIONKEY','C_PHONE','C_ACCTBAL','C_MKTSEGMENT','C_COMMENT'])
        ,"orders":(9,['O_ORDERKEY','O_CUSTKEY','O_ORDERSTATUS','O_TOTALPRICE','O_ORDERDATE','O_ORDERPRIORITY','O_CLERK','O_SHIPPRIORITY','O_COMMENT'])
        ,"lineitem":(16,['L_ORDERKEY','L_PARTKEY','L_SUPPKEY','L_LINENUMBER','L_QUANTITY','L_EXTENDEDPRICE','L_DISCOUNT','L_TAX','L_RETURNFLAG','L_LINESTATUS','L_SHIPDATE','L_COMMITDATE','L_RECEIPTDATE','L_SHIPINSTRUCT','L_SHIPMODE','L_COMMENT'])
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

def run_script(filename,cursor):
	


	
	try:
		with open(filename, 'r') as file:
			script_content = file.read()

			result = cursor.executescript(script_content)
	except sqlite3.Error as e:
			logging.exception("Database error: %s" % e) 
	except Exception as e:
			logging.exception("Exception in _query: %s" % e)




	
def read_files(rootdir,file_extension,delimit_char,broken_list,cursor):



	messages=[]
	for folder, subs, files in os.walk(rootdir):

		
		for filename in files :
			
			full_file=os.path.join(folder, filename)
			if file_extension in full_file and full_file not in broken_list:
					
				reader = csv.reader(open(full_file), delimiter=delimit_char)
				
				table_name=filename[:-4]
				field_count=dic[table_name][0]
				field_tuple=tuple(dic[table_name][1])
	
				i = 0
				for row in reader:
					i+=1
					messages = [field_tuple for msg in row]
					print(row)
					print(messages)
					result = cursor.executemany('INSERT INTO ' + table_name + ' VALUES ('+ ('?,' * len(dic)).strip(",") + ')',  messages)
					#print('INSERT INTO ' + table_name + ' VALUES ('+ ('?,' * len(dic)).strip(",") + ')')
				print(i, "rows from", filename, "uploaded into database")
	if conn:
		conn.close()

conn = init_conn(output_db)
cursor = conn.cursor()

bad_files=get_files("C:\PersonalProj\interviews\interview-test-data-engineer\data",".tbl","|")[1]

#
run_script("ddl_if not exists.sql",cursor)

read_files("C:\PersonalProj\interviews\interview-test-data-engineer\data",".tbl","|",bad_files,cursor)

# bulk insert the messages
messages = [(msg.id, msg.string) for msg in cat]
result = cursor.executemany('INSERT INTO ru  VALUES (?, ?)',  messages)
assert(result.rowcount == len(messages))
conn.commit()


