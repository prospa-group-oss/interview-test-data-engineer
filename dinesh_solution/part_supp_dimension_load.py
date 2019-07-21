import MySQLdb
import os

def main():
    load_part_supp_dimension()


def load_part_supp_dimension():
    path = os.path.dirname(os.path.realpath(__file__))
    fd = open(file="{}/datawarehouse/partsupplierdim.sql".format(path), mode="r")
    drop_partsuppdim_table = 'DROP TABLE IF EXISTS RETAIL.PARTSUPPLIERDIM'
    create_partsuppdim_table = fd.read()
    insert_partsuppdim_table = '''INSERT INTO PARTSUPPLIERDIM
      SELECT PS.PS_PARTKEY,    
      PS.PS_SUPPKEY    ,
      PS.PS_AVAILQTY   ,
      PS.PS_SUPPLYCOST ,
      PS.PS_COMMENT    ,
      S.S_NAME     ,
      S.S_ADDRESS   ,
      S.S_NATIONKEY ,
      S.S_PHONE     ,
      S.S_ACCTBAL   ,
      S.S_COMMENT   ,
      P.P_NAME        ,
      P.P_MFGR        ,
      P.P_BRAND       ,
      P.P_TYPE        ,
      P.P_SIZE        ,
      P.P_CONTAINER   ,
      P.P_RETAILPRICE ,
      P.P_COMMENT     
      FROM PARTSUPP PS, SUPPLIER S, PART P
      WHERE PS.PS_PARTKEY = P.P_PARTKEY
      AND PS.PS_SUPPKEY = S.S_SUPPKEY'''
    fd.close()

    connection = MySQLdb.connect(host="localhost", user="root", passwd='', database='retail')
    cursor = connection.cursor()

    try:
         try:
             cursor.execute(drop_partsuppdim_table)
             cursor.execute(create_partsuppdim_table)
             cursor.execute(insert_partsuppdim_table)
             connection.commit()
         except (MySQLdb.Error, MySQLdb.Warning) as e:
             print(e)
             return None
    finally:
        connection.close()

if __name__ == "__main__":
    main()