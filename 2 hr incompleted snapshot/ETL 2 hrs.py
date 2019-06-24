import sqlite3
import logging
import csv
import os
import data_validation

# Dict for Number of fields in each table
dic = {

        "NATION":(4,['N_NATIONKEY','N_NAME','N_REGIONKEY','N_COMMENT'])
        ,"REGION":(3,['R_REGIONKEY','R_NAME','R_COMMENT'])

        ,"PART":(9,['P_PARTKEY','P_NAME','P_MFGR','P_BRAND','P_TYPE','P_SIZE','P_CONTAINER','P_RETAILPRICE','P_COMMENT'])
        ,"SUPPLIER":(7,['S_SUPPKEY','S_NAME','S_ADDRESS','S_NATIONKEY','S_PHONE','S_ACCTBAL','S_COMMENT'])
        ,"PARTSUPP":(5,['PS_PARTKEY','PS_SUPPKEY','PS_AVAILQTY','PS_SUPPLYCOST','PS_COMMENT'])
        ,"CUSTOMER":(8,['C_CUSTKEY','C_NAME','C_ADDRESS','C_NATIONKEY','C_PHONE','C_ACCTBAL','C_MKTSEGMENT','C_COMMENT'])
        ,"ORDERS":(9,['O_ORDERKEY','O_CUSTKEY','O_ORDERSTATUS','O_TOTALPRICE','O_ORDERDATE','O_ORDERPRIORITY','O_CLERK','O_SHIPPRIORITY','O_COMMENT'])
        ,"LINEITEM":(16,['L_ORDERKEY','L_PARTKEY','L_SUPPKEY','L_LINENUMBER','L_QUANTITY','L_EXTENDEDPRICE','L_DISCOUNT','L_TAX','L_RETURNFLAG','L_LINESTATUS','L_SHIPDATE','L_COMMITDATE','L_RECEIPTDATE','L_SHIPINSTRUCT','L_SHIPMODE','L_COMMENT'])
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
				
				table_name=filename[:-4]
				field_count=dic[table_name][0]
				field_tuple=make_tuple(dic[table_name][0])
	
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


