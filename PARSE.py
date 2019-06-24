import re
#[re.findall(r'[f\(\s*([^,]+)\s*,\s*([^,]+)\s*\)]',line) 
print([re.findall(r'\s+(.*?)\s.*',line) 
            for line in open('inserts.txt')])