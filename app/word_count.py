import pyspark.sql.functions as F

from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName('Spark Structured Streaming')
    .getOrCreate()
)

spark.sparkContext.setLogLevel('ERROR')

# input data
lines = (
    spark
    .readStream
    .format('socket')
    .option('host', 'localhost')
    .option('port', 9999)
    .load()
)

print(type(lines))

# query
words = lines.select(
    F.explode(
        F.split(lines.value, ' ')
    ).alias('word')
)

word_counts = words.groupBy('word').count()

# output
query = (
    word_counts
    .writeStream
    .outputMode('complete')
    .format('console')
    .start()
)

query.awaitTermination()