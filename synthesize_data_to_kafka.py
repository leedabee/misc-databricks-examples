# Databricks notebook source
# MAGIC %md
# MAGIC ### Synthesize data 
# MAGIC 
# MAGIC [Databricks Labs Data Generator](https://databrickslabs.github.io/dbldatagen/public_docs/index.html) provides a convenient way to generate large volumes of synthetic test data from within a Databricks notebook (or regular Spark application).
# MAGIC 
# MAGIC I am using this to synthesize a data set to load into a kafka topic. Alternatively, you could read a parquet file in as a dataframe and then write this to a kafka topic. 

# COMMAND ----------

# DBTITLE 1,Install data generator
pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Configure Kafka Brokers
kafka_bootstrap_servers_tls = dbutils.secrets.get("lee","kafka_bootstrap_servers_tls")

# COMMAND ----------

# DBTITLE 1,Create your a Kafka topic unique to your name
# Full username, e.g. "john.doe@databricks.com"
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# Short form of username, suitable for use as part of a topic name.
user = username.split("@")[0].replace(".","_")

# DBFS directory for this project, we will store the Kafka checkpoint in there
project_dir = f"/home/{username}/oetrta/kafka_test"
checkpoint_location = f"{project_dir}/kafka_checkpoint"

topic = f"{user}_oetrta_kafka_test"

# COMMAND ----------

print( username )
print( user )
print( project_dir )
print( checkpoint_location )
print( topic )

# COMMAND ----------

import dbldatagen as dg
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Generate test data

row_count = 1000 * 100
column_count = 10
test_data_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=row_count,
                                  partitions=4, randomSeedMethod='hash_fieldname')
                   .withIdOutput()
                   .withColumn("r", FloatType(), expr="floor(rand() * 350) * (86400 + 3600)",
                                    numColumns=column_count)
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
                   .withColumn("code2", IntegerType(), minValue=0, maxValue=10, random=True)
                   .withColumn("code3", StringType(), values=['online', 'offline', 'unknown'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True, 
                               percentNulls=0.05)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, 
                                weights=[9, 1, 1])
                   )


df = test_data_spec.build(withStreaming=True, options={"rowsPerSecond": 100})
dbgen_df = df.withColumn("processingTime", lit(datetime.now().timestamp()).cast("timestamp")).withColumn("eventId", uuidUdf())

stream_schema = dbgen_df.schema

# COMMAND ----------

# DBTITLE 1,Write Stream Data to Kafka
# Clear checkpoint location for a clean start to the stream
# dbutils.fs.rm(checkpoint_location, True)

(
  dbgen_df.select(col("eventId").alias("key"), to_json(struct([dbgen_df[x] for x in dbgen_df.columns])).alias("value"))
   .writeStream
   .format("kafka")
   .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls)
   .option("kafka.security.protocol", "SSL") 
   .option("checkpointLocation", checkpoint_location )
   .option("topic", topic)
   .start()
)

# COMMAND ----------

# DBTITLE 1,Read Stream from Kafka
startingOffsets = "earliest"

kafka_df = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_tls) 
  .option("kafka.security.protocol", "SSL") 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .load())

read_stream = kafka_df.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), stream_schema).alias("json"))

display(read_stream)

# COMMAND ----------


