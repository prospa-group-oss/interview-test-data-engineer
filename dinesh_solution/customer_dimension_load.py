import MySQLdb
import os

def main():
    load_lineitem_fact()


def load_lineitem_fact():
    path = os.path.dirname(os.path.realpath(__file__))
    fd = open(file="{}/datawarehouse/customerdim.sql".format(path), mode="r")
    drop_customerdim_table = 'DROP TABLE IF EXISTS RETAIL.CUSTOMERDIM'
    create_customerdim_table = fd.read()
    insert_customerdim_table = '''INSERT INTO CUSTOMERDIM
      SELECT C.C_CUSTKEY,
      C.C_NAME,
      N.N_NAME,
      R.R_NAME,
      C.C_ADDRESS,
      C.C_PHONE,
      C.C_ACCTBAL,
      case when C.C_ACCTBAL <=2000 THEN "LOW" 
           when C.C_ACCTBAL >=2000 AND C.C_ACCTBAL<=5000 THEN "MID" 
           ELSE "HIGH"
      END AS BALANCE_CATEGORY,
      C.C_MKTSEGMENT,
      C.C_COMMENT,
      N.N_COMMENT,
      R.R_COMMENT
      FROM CUSTOMER C,  NATION N, REGION R
     WHERE C.C_NATIONKEY = N.N_NATIONKEY
     AND N.N_REGIONKEY = R.R_REGIONKEY'''
    fd.close()

    connection = MySQLdb.connect(host="localhost", user="root", passwd='', database='retail')
    cursor = connection.cursor()

    try:
         try:
             cursor.execute(drop_customerdim_table)
             cursor.execute(create_customerdim_table)
             cursor.execute(insert_customerdim_table)
             connection.commit()
         except (MySQLdb.Error, MySQLdb.Warning) as e:
             print(e)
             return None
    finally:
        connection.close()

if __name__ == "__main__":
    main()