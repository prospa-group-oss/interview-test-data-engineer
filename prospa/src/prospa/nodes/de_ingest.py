from kedro.contrib.io.pyspark import SparkDataSet

def ingest_nation(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["N_NATIONKEY",
                   "N_NAME",
                   "N_REGIONKEY",
                   "N_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df


def ingest_region(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["R_REGIONKEY",
                   "R_NAME",
                   "R_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df


def ingest_part(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["P_PARTKEY",
                   "P_NAME",
                   "P_MFGR",
                   "P_BRAND",
                   "P_TYPE",
                   "P_SIZE",
                   "P_CONTAINER",
                   "P_RETAILPRICE",
                   "P_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df


def ingest_supplier(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["S_SUPPKEY",
                   "S_NAME",
                   "S_ADDRESS",
                   "S_NATIONKEY",
                   "S_PHONE",
                   "S_ACCTBAL",
                   "S_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df


def ingest_partsupp(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["PS_PARTKEY",
                   "PS_SUPPKEY",
                   "PS_AVAILQTY",
                   "PS_SUPPLYCOST",
                   "PS_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df


def ingest_customer(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["C_CUSTKEY",
                   "C_NAME",
                   "C_ADDRESS",
                   "C_NATIONKEY",
                   "C_PHONE",
                   "C_ACCTBAL",
                   "C_MKTSEGMENT",
                   "C_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df


def ingest_orders(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["O_ORDERKEY",
                   "O_CUSTKEY",
                   "O_ORDERSTATUS",
                   "O_TOTALPRICE",
                   "O_ORDERDATE",
                   "O_ORDERPRIORITY",
                   "O_CLERK",
                   "O_SHIPPRIORITY",
                   "O_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df

def ingest_orders(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["O_ORDERKEY",
                   "O_CUSTKEY",
                   "O_ORDERSTATUS",
                   "O_TOTALPRICE",
                   "O_ORDERDATE",
                   "O_ORDERPRIORITY",
                   "O_CLERK",
                   "O_SHIPPRIORITY",
                   "O_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df


def ingest_lineitem(df: SparkDataSet) -> SparkDataSet:
    """
    Args:
        df: Source data frame

    Returns:
        Spark data frame
    """

    column_name = ["L_ORDERKEY",
                   "L_PARTKEY",
                   "L_SUPPKEY",
                   "L_LINENUMBER",
                   "L_QUANTITY",
                   "L_EXTENDEDPRICE",
                   "L_DISCOUNT",
                   "L_TAX",
                   "L_RETURNFLAG",
                   "L_LINESTATUS",
                   "L_SHIPDATE",
                   "L_COMMITDATE",
                   "L_RECEIPTDATE",
                   "L_SHIPINSTRUCT",
                   "L_SHIPMODE",
                   "L_COMMENT"]
    df = df.drop(df.columns[-1]).toDF(*column_name)

    return df
