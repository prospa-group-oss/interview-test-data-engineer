# -*- coding: utf-8 -*-

# Please see read.md for instructions

import csv
import os

# Check number of rows, and number of columns
def validation(filename,delimit_char):

	reader = csv.reader(open(filename), delimiter=delimit_char)
	value_set=set()
	for index,row in enumerate(reader):
		value_set.add(len(row))
		

	
	return value_set,index+1

# Return tuple element 1 is a list of (filename,number of columns, number of rows)
# element 2 is a list of files that is corrupted because delimitor didn't work, more or fewer columns occured
def get_files(rootdir,file_extension,delimit_char):



	result=[]
	broken_files=[]
	for folder, subs, files in os.walk(rootdir):

		for filename in files:
			if file_extension in filename:
					full_file=os.path.join(folder, filename)
					file_result=validation(full_file,delimit_char)
					result.append((full_file,file_result[0],file_result[1]))
					if len(file_result[0])>1:
							broken_files.append(full_file) 
	return result,broken_files
                  

print(get_files("C:\PersonalProj\interviews\interview-test-data-engineer\data",".tbl","|")[0])
print(get_files("C:\PersonalProj\interviews\interview-test-data-engineer\data",".tbl","|")[1])