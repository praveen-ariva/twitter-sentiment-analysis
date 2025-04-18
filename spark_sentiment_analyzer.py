from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Download NLTK resources for sentiment analysis
nltk.download('vader_lexicon')

# Initialize sentiment analyzer
sia = SentimentIntensityAnalyzer()

# User-defined function for sentiment analysis
def analyze_sentiment(text):
    if not text:
        return "neutral"
    
    sentiment_score = sia.polarity_scores(text)
    compound_score = sentiment_score['compound']
    
    if compound_score >= 0.05:
        return "positive"
    elif compound_score <= -0.05:
        return "negative"
    else:
        return "neutral"

# Register the UDF with Spark
sentiment_udf = udf(analyze_sentiment, StringType())

def main():
    # Create Spark session
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created")
    
    # Define schema for the tweet data coming from Kafka
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("text", StringType(), True),
        StructField("user", StringType(), True),
        StructField("followers_count", IntegerType(), True),
        StructField("location", StringType(), True),
        StructField("lang", StringType(), True)
    ])
    
    # Read from Kafka
    tweets_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitter_tweets") \
        .option("startingOffsets", "latest") \
        .load()
    
    logger.info("Kafka stream connected")
    
    # Parse JSON from value column
    parsed_tweets_df = tweets_df \
        .select(from_json(col("value").cast("string"), schema).alias("tweet")) \
        .select("tweet.*")
    
    # Apply sentiment analysis
    sentiment_df = parsed_tweets_df \
        .withColumn("sentiment", sentiment_udf(col("text"))) \
        .select("id", "created_at", "text", "user", "followers_count", "location", "lang", "sentiment")
    
    # Count by sentiment (for monitoring)
    sentiment_counts = sentiment_df \
        .groupBy(window(col("created_at").cast("timestamp"), "10 minutes", "5 minutes"), col("sentiment")) \
        .count() \
        .orderBy("window", "sentiment")
    
    # Write the detailed sentiment data to console for debugging
    query = sentiment_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    # Write the count results to console
    count_query = sentiment_counts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    # Output to file for persistence
    file_query = sentiment_df \
        .writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", "output/tweets_sentiment") \
        .option("checkpointLocation", "checkpoints/tweets_sentiment") \
        .start()
    
    logger.info("Streaming queries started")
    
    # Wait for termination
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()