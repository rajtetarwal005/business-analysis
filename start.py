from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType
from pyspark.sql.functions import month, year, quarter
from setuptools.command.alias import alias
from pyspark.sql import functions as F
from pyspark.sql.functions import col

spark_session = SparkSession.builder.appName('business analysis').getOrCreate()
print("hello")
# data read
# customer data
schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("location", StringType(), True),
    StructField("Source_order", StringType(), True),
])
print(schema)
customer_df = spark_session.read.csv("/home/tetarwal005/Desktop/business analysis/data/custumerdetails.txt",schema=schema)
customer_df = customer_df.withColumn("month",month("order_date"))
customer_df = customer_df.withColumn("year",year("order_date"))
customer_df = customer_df.withColumn("quarter",quarter("order_date"))
customer_df.show(50)

# menu data
schema1 = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", StringType(), True),
])

menu_df = spark_session.read.csv("/home/tetarwal005/Desktop/business analysis/data/menu.csv.txt",schema=schema1)
menu_df.show()

# total amount spent by each customer
customer_menu_df = customer_df.join(menu_df, "product_id" ,"inner").drop(menu_df.product_id)
# customer_menu_df.show()
# total_amt = customer_menu_df.groupBy("customer_id").agg(F.sum("price").cast("int").alias("amount")).orderBy("amount",ascending=False)
# # total_amt.show()

# total amount spent by each food category
# food_category = customer_menu_df.groupby("product_name").agg(F.sum("price").cast("int").alias("amount"))
# # food_category.show()

# # total amount of sales in each month
# each_month = customer_menu_df.groupby("month").agg(F.sum("price").cast("int").alias("amount")).orderBy("month")
# # each_month.show()

# total number of order by each category
each_category =customer_menu_df.groupby("product_name").agg(F.count("product_id").alias("number of product")).orderBy("number of product",ascending=False)
# each_category.show()

# top ordered item
each_category_top =customer_menu_df.groupby("product_name").agg(F.count("product_id").alias("number of product")).orderBy("number of product",ascending=False).limit(1)
# each_category_top.show()

# frequency of customer visited to restaurant
frequency_of_customer =customer_df.filter(col("Source_order")=="Restaurant").groupby("customer_id").agg(F.count("order_date").alias("frequency of customer"))
frequency_of_customer.show()

# total sales by each country
each_country = customer_menu_df.groupby("location").agg(F.sum("price").alias("total sale"))
each_country.show()

each_month = customer_menu_df.groupby("month").agg(F.sum("price").cast("int").alias("amount")).orderBy("month")
each_month.show()

total_amt = customer_menu_df.groupBy("customer_id").agg(F.sum("price").cast("int").alias("amount")).orderBy("amount",ascending=False)
total_amt.show()

food_category = customer_menu_df.groupby("product_name").agg(F.sum("price").cast("int").alias("amount"))
food_category.show()
# this project is made by raj tetarwal
