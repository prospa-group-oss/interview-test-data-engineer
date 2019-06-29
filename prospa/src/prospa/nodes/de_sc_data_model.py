from kedro.contrib.io.pyspark import SparkDataSet
from pyspark.sql import functions as f

def customer_dimension(df_customer: SparkDataSet,
                       df_region: SparkDataSet,
                       df_nation: SparkDataSet) -> SparkDataSet:
    """
    Args:
        **df: Source data frames

    Returns:
        Spark data frame
    """

    column_name = [
        "Customer_Key",
        "Customer_Name",
        "Customer_Address",
        "Customer_Phone",
        "Customer_Account_Balance",
        "Customer_Marketing_Segment",
        "Customer_Country_Name",
        "Customer_Region_Name"
    ]
    df_customer_dimension = df_customer.join(df_nation, df_customer.C_NATIONKEY == df_nation.N_NATIONKEY) \
        .join(df_region, df_region.R_REGIONKEY == df_nation.N_REGIONKEY) \
        .drop("C_NATIONKEY",
              "N_NATIONKEY",
              "N_REGIONKEY",
              "R_REGIONKEY",
              "C_COMMENT",
              "N_COMMENT",
              "R_COMMENT") \
        .toDF(*column_name) \
        .withColumn("Customer_Account_Balance_Group",
                    f.when(f.col("Customer_Account_Balance") < 4000, "Less than 3000") \
                    .when(f.col("Customer_Account_Balance") < 8000, "Between 3000 and 8000") \
                    .otherwise("More than 8000"))

    return df_customer_dimension


def part_dimension(df_part: SparkDataSet) -> SparkDataSet:
    """
    Args:
        **df: Source data frames

    Returns:
        Spark data frame
    """

    column_name = [
        "Part_Key",
        "Part_Name",
        "Part_Manufacturer",
        "Part_Brand",
        "Part_Type",
        "Part_Size",
        "Part_Container",
        "Part_Retail_Price"
    ]
    df_part_dimension = df_part.drop("P_COMMENT").toDF(*column_name)

    return df_part_dimension


def supplier_dimension(df_supplier: SparkDataSet,
                       df_region: SparkDataSet,
                       df_nation: SparkDataSet) -> SparkDataSet:
    """
    Args:
        **df: Source data frames

    Returns:
        Spark data frame
    """

    column_name = [
        "Supplier_Key",
        "Supplier_Name",
        "Supplier_Address",
        "Supplier_Phone",
        "Supplier_Account_Balance",
        "Supplier_Country_Name",
        "Supplier_Region_Name"
    ]
    df_supplier_dimension = df_supplier.join(df_nation, df_supplier.S_NATIONKEY == df_nation.N_NATIONKEY) \
        .join(df_region, df_region.R_REGIONKEY == df_nation.N_REGIONKEY) \
        .drop("S_COMMENT",
              "N_COMMENT",
              "R_COMMENT",
              "N_NATIONKEY",
              "N_REGIONKEY",
              "S_NATIONKEY",
              "R_REGIONKEY"
              ) \
        .toDF(*column_name)

    return df_supplier_dimension


def order_fact(df_orders: SparkDataSet,
               df_lineitem: SparkDataSet,
               df_partsupp: SparkDataSet) -> SparkDataSet:
    """
    Args:
        **df: Source data frames

    Returns:
        Spark data frame
    """

    column_name = [
        "Order_Customer_Key",
        "Order_Status",
        "Order_Total_Price",
        "Order_Date",
        "Order_Priority",
        "Order_Clerk",
        "Order_Ship_Priority",
        "Order_Line_Number",
        "Order_Line_Quantity",
        "Order_Line_Extended_Price",
        "Order_Line_Discount",
        "Order_Line_Tax",
        "Order_Return_Flag",
        "Order_Line_Status",
        "Order_Ship_Date",
        "Order_Commit_Date",
        "Order_Receipt_Date",
        "Order_Shipping_Instruction",
        "Order_Ship_Mode",
        "Order_Part_Key",
        "Order_Supplier_Key",
        "Order_Available_Quantity",
        "Order_Supply_Cost",
        "Order_Line_Revenue"
    ]
    df_order_fact = df_orders.join(df_lineitem, df_orders.O_ORDERKEY == df_lineitem.L_ORDERKEY) \
        .join(df_partsupp, (df_lineitem.L_PARTKEY == df_partsupp.PS_PARTKEY) &
              (df_lineitem.L_SUPPKEY == df_partsupp.PS_SUPPKEY)) \
        .drop("O_COMMENT",
              "L_COMMENT",
              "PS_COMMENT",
              "L_PARTKEY",
              "L_SUPPKEY",
              "O_ORDERKEY",
              "L_ORDERKEY",
              "R_REGIONKEY"
              ) \
        .withColumn("Order_Line_Revenue", df_lineitem.L_EXTENDEDPRICE*(1-df_lineitem.L_DISCOUNT) +
                    df_lineitem.L_EXTENDEDPRICE * df_lineitem.L_TAX) \
        .toDF(*column_name)

    return df_order_fact

