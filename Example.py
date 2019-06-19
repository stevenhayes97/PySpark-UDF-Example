from pyspark.sql.types import StringType
from pyspark.sql import Row

def my_function(x):
  return x + "test"


# Python Name - myPythonUDF
# SQL Name - nameForSQLUDF
# The original funciton name - my_function
# The function's return type - StringType()

myPythonUDF = spark.udf.register("nameForSQLUDF", my_function, StringType())

# Create a test dataframe

data = spark.createDataFrame([Row(test1='xxxxx', test2='yyyyyy'),
 Row(test1='asdfasdf', test2='dasffaac'),
 Row(test1='jjjfjf', test2='fffee3'),
 Row(test1='xzsef4', test2='farq2345w')])

UDFwithDF = data.select("*", myPythonUDF("test1").alias("test1WithUDF"))

# Displays the dataframe in Databricks where I tested this example
display(UDFwithDF)



########################################
Run this in SQL
########################################

# Create a temp table from PySpark dataframe

data.createOrReplaceTempView("data_table")

SELECT test1,
  test2,
  nameForSQLUDF(test1) as test1WithUDF
FROM
  data_table
