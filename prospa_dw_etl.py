#!/usr/bin/python
 
import sqlite3
from sqlite3 import Error
 
 
def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return None

def load_dim_calendar(conn):
    """
    load dim_customer table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("WITH RECURSIVE dates(date) AS ( \
              VALUES('1992-01-01') \
              UNION ALL \
              SELECT date(date, '+1 day') \
              FROM dates \
              WHERE date < '1999-01-01' \
            ) \
            insert into DIM_CALENDAR(CALENDAR_DT,MONTH,YEAR) \
            SELECT date,strftime('%m',date) ,strftime('%Y',date)  \
             FROM dates \
            where not exists \
            ( \
                select 1 from DIM_CALENDAR  where calendar_dt = date \
            )"
    )
    
    conn.commit
    print('dim_calendar loaded')
    
 
def load_dim_customer(conn):
    """
    load dim_customer table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("insert into DIM_CUSTOMER (CUSTOMER_KEY,NAME,ADDRESS,PHONE,MKTSEGMENT,BALANCE,BALANCE_CLASS,NATION_NAME,REGION_NAME) \
        select CUSTOMER.C_CUSTKEY,CUSTOMER.C_NAME,CUSTOMER.C_ADDRESS,CUSTOMER.C_PHONE,CUSTOMER.C_MKTSEGMENT,CUSTOMER.C_ACCTBAL,case when C_ACCTBAL <0 then 'Negative' when C_ACCTBAL between 0 and 5000 then '0-5000' else '>5000' end,NATION.N_NAME,REGION.R_NAME \
        from CUSTOMER \
        inner join NATION on CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY \
        inner join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY \
        where not exists \
        ( \
            select 1 from DIM_CUSTOMER where DIM_CUSTOMER.CUSTOMER_KEY = CUSTOMER.C_CUSTKEY \
        )"
    )
    
    conn.commit
    
    cur.execute("update  DIM_CUSTOMER \
        set (NAME,ADDRESS,PHONE,MKTSEGMENT,BALANCE,BALANCE_CLASS,NATION_NAME,REGION_NAME) = ( \
            select CUSTOMER.C_NAME,CUSTOMER.C_ADDRESS,CUSTOMER.C_PHONE,CUSTOMER.C_MKTSEGMENT,CUSTOMER.C_ACCTBAL,case when C_ACCTBAL <0 then 'Negative' when C_ACCTBAL between 0 and 5000 then '0-5000' else '>5000' end,NATION.N_NAME,REGION.R_NAME \
                from CUSTOMER \
                inner join NATION on CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY \
                inner join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY \
            where DIM_CUSTOMER.CUSTOMER_KEY = CUSTOMER.C_CUSTKEY \
        ) \
        where exists \
        ( \
            select 1 from CUSTOMER where DIM_CUSTOMER.CUSTOMER_KEY = CUSTOMER.C_CUSTKEY \
        )"
    )
    
    conn.commit
 
    print('dim_customer loaded')
 
def load_dim_supplier(conn):
    """
    load DIM_SUPPLIER table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("insert into DIM_SUPPLIER (SUPPLIER_KEY,NAME,ADDRESS,NATION_NAME,REGION_NAME,PHONE) \
        select SUPPLIER.S_SUPPKEY,SUPPLIER.S_NAME,SUPPLIER.S_ADDRESS,NATION.N_NAME,REGION.R_NAME,SUPPLIER.S_PHONE \
            from SUPPLIER \
            inner join NATION on SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY \
            inner join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY \
            where not exists \
            ( \
                select 1 from DIM_SUPPLIER where DIM_SUPPLIER.SUPPLIER_KEY = SUPPLIER.S_SUPPKEY \
            )"
    )
    
    conn.commit
    
    cur.execute("update  DIM_SUPPLIER \
        set (NAME,ADDRESS,NATION_NAME,REGION_NAME,PHONE) = ( \
            select SUPPLIER.S_NAME,SUPPLIER.S_ADDRESS,NATION.N_NAME,REGION.R_NAME,SUPPLIER.S_PHONE \
                from SUPPLIER \
                inner join NATION on SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY \
                inner join REGION on NATION.N_REGIONKEY = REGION.R_REGIONKEY \
            where DIM_SUPPLIER.SUPPLIER_KEY = SUPPLIER.S_SUPPKEY \
        ) \
        where exists \
        ( \
            select 1 from SUPPLIER where DIM_SUPPLIER.SUPPLIER_KEY = SUPPLIER.S_SUPPKEY \
        )"
    )
    
    conn.commit
 
    print('dim_supplier loaded')
 
def load_dim_part(conn):
    """
    load DIM_PART table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("insert into DIM_PART (PART_KEY,NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER, RETAILPRICE) \
        select P_PARTKEY,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE \
            from PART \
            where not exists \
            ( \
                select 1 from DIM_PART where DIM_PART.PART_KEY = PART.P_PARTKEY \
            )"
    )
    
    conn.commit
    
    cur.execute(" update DIM_PART \
            set (NAME,MFGR,BRAND,TYPE,SIZE,CONTAINER, RETAILPRICE) = ( \
                select P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE \
                from PART \
                where DIM_PART.PART_KEY = PART.P_PARTKEY \
            ) \
            where exists \
            ( \
                select 1 from PART where DIM_PART.PART_KEY = PART.P_PARTKEY \
            )"
    )
    
    conn.commit
 
    print('dim_part loaded')

def load_fact_order_lineitems(conn):
    """
    load FACT_ORDER_LINEITEMS table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute("insert into FACT_ORDER_LINEITEMS (ORDERKEY,LINENUMBER,CUSTOMER_KEY,SUPPLIER_KEY,PART_KEY,ORDERSTATUS,ORDER_TOTALPRICE,ORDERDATE,ORDERPRIORITY,OORDER_CLERK,ORDER_SHIPPRIORITY,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPMODE,LINE_REVENUE) \
            select L_ORDERKEY,L_LINENUMBER,O_CUSTKEY, L_SUPPKEY, L_PARTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPMODE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) \
            from ORDERS \
            inner join LINEITEM on ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY \
            where not exists  \
            ( \
                select 1 from FACT_ORDER_LINEITEMS \
                    where  FACT_ORDER_LINEITEMS.ORDERKEY = ORDERS.O_ORDERKEY and FACT_ORDER_LINEITEMS.LINENUMBER = LINEITEM.L_LINENUMBER \
            )"
    )
    
    conn.commit
    
    cur.execute("update FACT_ORDER_LINEITEMS \
                set (CUSTOMER_KEY,SUPPLIER_KEY,PART_KEY,ORDERSTATUS,ORDER_TOTALPRICE,ORDERDATE,ORDERPRIORITY,OORDER_CLERK,ORDER_SHIPPRIORITY,QUANTITY,EXTENDEDPRICE,DISCOUNT,TAX,RETURNFLAG,LINESTATUS,SHIPDATE,COMMITDATE,RECEIPTDATE,SHIPMODE,LINE_REVENUE) = ( \
                    select O_CUSTKEY, L_SUPPKEY, L_PARTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPMODE,(L_EXTENDEDPRICE*(1-L_DISCOUNT)) \
                        from ORDERS \
                    inner join LINEITEM on ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY \
                    where FACT_ORDER_LINEITEMS.ORDERKEY = ORDERS.O_ORDERKEY and FACT_ORDER_LINEITEMS.LINENUMBER = LINEITEM.L_LINENUMBER \
                ) \
                where exists \
                ( \
                    select 1 from ORDERS \
                    inner join LINEITEM on ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY \
                    where FACT_ORDER_LINEITEMS.ORDERKEY = ORDERS.O_ORDERKEY and FACT_ORDER_LINEITEMS.LINENUMBER = LINEITEM.L_LINENUMBER \
                )"
    )
    
    conn.commit
 
    print('fact_order_lineitems loaded')
    
 
def main():
    database = "C:\\Software\\sqlite\\TPC-H.db"
 
    # create a database connection
    conn = create_connection(database)
    with conn:
    
        print("1. load dim_calendar:")
        load_dim_calendar(conn)
        
        print("2. load dim_customer:")
        load_dim_customer(conn)
 
        print("3. load dim_supplier")
        load_dim_supplier(conn)
        
        print("4. load dim_part")
        load_dim_part(conn)
        
        print("5. load fact_order_lineitems")
        load_fact_order_lineitems(conn)
    
    conn.close
 
if __name__ == '__main__':
    main()