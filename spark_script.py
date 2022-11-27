from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,avg,max,min,mean,count

spark= SparkSession \
    .builder \
    .getOrCreate()

df = spark.read.option("header", True).csv("/home/ec2-user/distributed-file-system/Consume_Price_Index.csv")

df_output = df.groupby('Year').agg(
    min('Value').alias('Min_Value'),
    max('Value').alias('Max_Value'),
    avg('Value').alias('Avg_Value'),
    sum('Value').alias('Sum_Value'),
)

df_output.write.option("header", True).mode('overwrite').csv("/home/ec2-user/distributed-file-system/PMR_Ouput.csv")