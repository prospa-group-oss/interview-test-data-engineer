import json
import os
import MySQLdb

def main():
    config = CONFIG
    load_data(config)

def get_config():
    config_file = None
    path = os.path.dirname(os.path.realpath(__file__))
    try:
        config_file = open(file="{}/parameters/config.json".format(path), mode="r")
        config_str = config_file.read()
        config_json = json.loads(config_str)
        return config_json
    finally:
        if (config_file!=None):
            config_file.close()

def execute_query(config, table):
    connection = MySQLdb.connect(host=config["host"], user=config["user"], passwd='')
    path = os.path.dirname(os.path.realpath(__file__))
    cursor = connection.cursor()
    query = "LOAD DATA LOCAL INFILE " \
            "'{}/landing/{}.tbl' " \
            "INTO TABLE {}.{} FIELDS TERMINATED BY '|'".format(path, table, config['database'], table)
    try:
         try:
             cursor.execute(query)
             connection.commit()
         except (MySQLdb.Error, MySQLdb.Warning) as e:
             print(e)
             return None
    finally:
        connection.close()

def load_data(config):
    for table in config['tables']:
        print("loading " + table + " table ")
        execute_query(config, table)

CONFIG = get_config()

if __name__ == "__main__":
    main()