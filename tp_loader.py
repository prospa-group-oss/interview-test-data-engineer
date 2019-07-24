import os
import logging
import tp_configuration.util as util
import dlayer.databaseconnection as dbconnection
import sqlite3


logger = logging.getLogger(__name__)

def tp_load():
    """

    :return:
    """
    try :
        #load configuration
        cust_config = util.load_config()
        #connect to database and create cursor
        db_connection = dbconnection.databaseConnection()

        logger.info("Started loading of the file to database.")

        for db_file in cust_config["file_name"]:
            count = 1
            complete_file_name = os.path.join(cust_config["file_path"],".".join([db_file,cust_config["filename_suffix"]]))
            with open(complete_file_name,'r') as db_open_file:
                for line in db_open_file.readlines():
                    db_connection.query(cust_config['insert_query'][db_file],line.split("|")[:-1])
                    if count%cust_config['info_threshold'] == 0 :
                        logger.error("Loaded {} records to {} table".format(count,db_file))
                    count+=1

            logger.error("Done loaded {} records to {} table".format(count, db_file))

    except FileNotFoundError as e :
        logger.error("{} is not correct or Not found. ".format(e.filename))
        raise(FileNotFoundError)

    except sqlite3.OperationalError as e:
        logger.error("Error while database connection. for {} error {} ".format(db_file,e))
        raise(sqlite3.OperationalError)

    except (sqlite3.ProgrammingError, sqlite3.IntegrityError) as e :
        logger.error("Error while inserting records. {} {} {}".format(db_file,cust_config['insert_query'][db_file],e))
        logger.error("record : {}".format(line))
        raise(e)
    except Exception as e:
        logger.error("Got Exception {} while processing".format(e))
        raise(Exception)


if __name__ == "__main__":
    tp_load()


