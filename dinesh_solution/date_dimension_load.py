import MySQLdb

def main():
     load_date_dimension()


def load_date_dimension():
    drop_small_table = 'DROP TABLE IF EXISTS retail.numbers_small;'
    create_small_table = 'CREATE TABLE retail.numbers_small (number INT);'
    insert_small_table = 'INSERT INTO retail.numbers_small VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);'

    drop_numbers_table = 'DROP TABLE IF EXISTS retail.numbers;'
    create_numbers_table = 'CREATE TABLE retail.numbers (number BIGINT);'
    insert_numbers_table = '''INSERT INTO retail.numbers
      SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number
      FROM numbers_small thousands, numbers_small hundreds, numbers_small tens, numbers_small ones
      LIMIT 1000000;'''

    drop_dates_table = "DROP TABLE IF EXISTS retail.dates;"
    create_dates_table = '''CREATE TABLE retail.dates (
        date_id          BIGINT PRIMARY KEY, 
        date             DATE NOT NULL,
        weekend          CHAR(10) NOT NULL DEFAULT "Weekday",
        day_of_week      CHAR(10) ,
        month            CHAR(10) ,
        month_day        INT , 
        year             INT ,
        week_starting_monday CHAR(2) ,
        UNIQUE KEY `date` (`date`),
        KEY `year_week` (`year`,`week_starting_monday`)
        );'''

    insert_dates_table = '''INSERT INTO retail.dates (date_id, date)
        SELECT number, DATE_ADD( '1990-01-01', INTERVAL number DAY )
        FROM numbers
        WHERE DATE_ADD( '1990-01-01', INTERVAL number DAY ) BETWEEN '1990-01-01' AND '2020-01-01'
        ORDER BY number;'''

    update_dates_table = '''UPDATE retail.dates SET
         day_of_week = DATE_FORMAT( date, "%W" ),
         weekend =     IF( DATE_FORMAT( date, "%W" ) IN ('Saturday','Sunday'), 'Weekend', 'Weekday'),
         month =       DATE_FORMAT( date, "%M"),
         year =        DATE_FORMAT( date, "%Y" ),
         month_day =   DATE_FORMAT( date, "%d" );'''

    update_dates_table_01 = '''UPDATE retail.dates SET week_starting_monday = DATE_FORMAT(date,'%v');'''

    connection = MySQLdb.connect(host="localhost", user="root", passwd='', database='retail')
    cursor = connection.cursor()

    try:
         try:
             cursor.execute(drop_small_table)
             cursor.execute(create_small_table)
             cursor.execute(insert_small_table)
             cursor.execute(drop_numbers_table)
             cursor.execute(create_numbers_table)
             cursor.execute(insert_numbers_table)
             cursor.execute(drop_dates_table)
             cursor.execute(create_dates_table)
             cursor.execute(insert_dates_table)
             cursor.execute(update_dates_table)
             cursor.execute(update_dates_table_01)

             connection.commit()
         except (MySQLdb.Error, MySQLdb.Warning) as e:
             print(e)
             return None
    finally:
        connection.close()

if __name__ == "__main__":
    main()