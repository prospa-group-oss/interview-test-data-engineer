import os
import logging
import tp_configuration.util as util
import dlayer.databaseconnection as dbconnection
import sqlite3


logger = logging.getLogger(__name__)

def dwh_loader():
    """

    :return:
    """
    try :
        #load configuration
        dwh_config = util.dwh_load_config()
        #connect to database and create cursor
        conn = dbconnection.databaseConnection()

        logger.info("Started moving records to dwh")

        for table in dwh_config:
            conn.query(dwh_config[table])
            logger.error("Done inserting records to {} table".format(table))

    except FileNotFoundError as e :
        logger.error("{} is not correct or Not found. ".format(e.filename))
        raise(FileNotFoundError)

    except sqlite3.OperationalError as e:
        logger.error("Error while database connection. for {} error {} ".format(table,e))
        raise(sqlite3.OperationalError)

    except (sqlite3.ProgrammingError, sqlite3.IntegrityError) as e :
        logger.error("Error while inserting records.  {} {}".format(table,e))


    except Exception as e:
        logger.error("Got Exception {} while processing".format(e))
        raise(Exception)


if __name__ == "__main__":
    dwh_loader()


