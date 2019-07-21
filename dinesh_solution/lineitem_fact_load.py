import MySQLdb
import os

def main():
    load_lineitem_fact()


def load_lineitem_fact():
    path = os.path.dirname(os.path.realpath(__file__))
    fd = open(file="{}/datawarehouse/lineitemfact.sql".format(path), mode="r")
    drop_lineitemfact_table = 'DROP TABLE IF EXISTS RETAIL.LINEITEMFACT'
    create_lineitemfact_table = fd.read()
    insert_lineitemfact_table = '''INSERT INTO LINEITEMFACT
       SELECT L.L_ORDERKEY,
       L.L_PARTKEY,
       L.L_SUPPKEY,
       O.O_CUSTKEY,
       L.L_LINENUMBER,
       O.O_ORDERSTATUS,
       O.O_ORDERDATE,
        L.L_QUANTITY,
        L.L_EXTENDEDPRICE,
        L.L_DISCOUNT,
        L.L_TAX,
        L.L_RETURNFLAG,
       L.L_LINESTATUS,
        L.L_SHIPDATE,
        L.L_COMMITDATE,
        L.L_RECEIPTDATE,
        L.L_SHIPINSTRUCT,
        L.L_SHIPMODE,
        L.L_COMMENT,
        L.L_QUANTITY * L.L_EXTENDEDPRICE AS LINEITEM_REVENUE_PER_ORDER
        FROM LINEITEM L, ORDERS O
         WHERE L.L_ORDERKEY = O.O_ORDERKEY'''
    fd.close()

    connection = MySQLdb.connect(host="localhost", user="root", passwd='', database='retail')
    cursor = connection.cursor()

    try:
         try:
             cursor.execute(drop_lineitemfact_table)
             cursor.execute(create_lineitemfact_table)
             cursor.execute(insert_lineitemfact_table)
             connection.commit()
         except (MySQLdb.Error, MySQLdb.Warning) as e:
             print(e)
             return None
    finally:
        connection.close()

if __name__ == "__main__":
    main()