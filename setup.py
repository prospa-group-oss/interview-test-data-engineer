from setuptools import setup
import dlayer.databaseconnection as dbconnection
import tp_configuration.util as util


conn = dbconnection.databaseConnection()
ddl = util.ddl_config()
for qry in ddl.values():
    conn.query(qry)



with open('requirements.txt') as f:
    requirements = f.read().splitlines()

version = '0.1'

setup(
    name='ETL',
    version=version,
    install_requires=requirements)