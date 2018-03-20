# Twitter & Kafka
Use Tweepy Python client to stream tweets to Kafka.

To install Tweepy

<code>pip install tweepy</code>

Avro data format for communication between Kafka and Spark Streaming.

Avro schema:

<pre><code>{
    "name": "TwitterEvent",
    "type": "record",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "source", "type": "string"},
        {"name": "text", "type": "string"},
        {"name": "lang", "type": "string"}
    ]
}
</code></pre>

To produce tweets to Kafka, use Confluent Python client:

<pre><code>pip install confluent-kafka
pip install avro # for encoding data to Avro
</code></pre>

# Spark Streaming

Install pyspark library:

<code>pip install pyspark</code>

Code snippet to start Spark Streaming:

<pre><code>batch_duration = 10 # 10 seconds
topic = 'twitter' # kafka topic

sc = SparkContext("local[2]", "SentimentAnalysisWithSpark")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, batch_duration) 
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {topic: 1}, valueDecoder=decode_tweet)
...
ssc.start()  
ssc.awaitTermination()
</code></pre>

**Note:** In other to KafkaUtils can understand Avro, we need to specify **valueDecoder** for the createStream method.

To run the code:

<code>spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.0 python_file_path</code>

This command is for Kafka v0.8 and Spark Streaming c2.3.0 on Scala 2.11. If you use diffirent versions, change package versions accordingly.

# Hbase

Hbase is used to store tweets.

We use three tables in the project:

Table 'tweets' for storing tweets

<code>CREATE 'tweets', 'tweet'</code>

Table 'neg_counter' for storing negative word frequency

<code>CREATE 'neg_counter', 'info'</code>

Table 'pos_counter' for storing positive word frequency

<code>CREATE 'pos_counter', 'info'</code>

# Hive

Hive is used to integrate SparkSQL and Hbase

We use three external tables to link with Hbase tables respectively:

<pre><code>CREATE EXTERNAL TABLE tweets(key string, text string, cleaned_text string, source string, lang string, target string) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,tweet:text,tweet:cleaned_text,tweet:source,tweet:lang,tweet:target")  
TBLPROPERTIES ("hbase.table.name" = "tweets");
</code></pre>


<pre><code>CREATE EXTERNAL TABLE neg_counter(key string, count bigint) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:counter#b") TBLPROPERTIES ("hbase.table.name" = "neg_counter");
</code></pre>

<pre><code>CREATE EXTERNAL TABLE pos_counter(key string, count bigint) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:counter#b") TBLPROPERTIES ("hbase.table.name" = "pos_counter");
</code></pre>


 # Spark SQL

To connect Spark SQL with Hbase:

<pre><code>from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Sentiment Query") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport().getOrCreate()
</code></pre>

And then query data:

<pre><code>query = spark.sql("SELECT target, count(target) AS count FROM tweets GROUP BY target")

# visualize data
df = query.toPandas()
df.plot(kind='pie', labels=df['target'], y='count', autopct='%1.1f%%', startangle=90, legend=False)
</code></pre>