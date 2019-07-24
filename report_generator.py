import tp_configuration.util as util
import logging
import dlayer.databaseconnection as dbconnection
import os
import sqlite3
logger = logging.getLogger(__name__)


def report_gen():
    """

    :return:
    """

    try :
        report_config = util.report_config()
        conn = dbconnection.databaseConnection()
        report_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)),"report")
        for report in report_config:
            report_file = os.path.join(report_directory,".".join([report,"txt"]))
            exe_query = conn.query(report_config[report]["query"])

            with open(report_file,"w") as rep_wrt:
                rep_wrt.write(report_config[report]["dilimiter"].join(report_config[report]["header"]))
                rep_wrt.write("\n")

                for line in exe_query.fetchall():
                    rep_wrt.write(report_config[report]["dilimiter"].join(map(str,line)))
                    rep_wrt.write("\n")
            logger.error("Generated {} report.".format(report))

    except FileNotFoundError as e :
        logger.error("{} is not correct or Not found. ".format(e.filename))
        raise(FileNotFoundError)

    except sqlite3.OperationalError as e:
        logger.error("Error while database connection. error {} ".format(e))
        raise(sqlite3.OperationalError)

    except (sqlite3.ProgrammingError, sqlite3.IntegrityError) as e :
        logger.error("Error while Fetching records. {}".format(e))

    except Exception as e:
        logger.error("Got error {}".format(e))
        raise(Exception)


if __name__ == "__main__":
    report_gen()