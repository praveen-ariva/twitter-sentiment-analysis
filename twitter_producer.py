import tweepy
import json
import time
from kafka import KafkaProducer

# Twitter API credentials
BEARER_TOKEN = "YOUR_BEARER_TOKEN"

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'twitter_tweets'

def create_kafka_producer():
    """Create and return a Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def main():
    """Main function to run the Twitter search"""
    try:
        # Create Kafka producer
        producer = create_kafka_producer()
        
        # Create Twitter client
        client = tweepy.Client(bearer_token=BEARER_TOKEN)
        
        print("Starting Twitter search...")
        
        # Keep searching for tweets periodically
        while True:
            try:
                # Search for tweets (this works with Basic access)
                search_results = client.search_recent_tweets(
                    query="python OR data science OR machine learning",
                    max_results=10,
                    tweet_fields=['created_at', 'author_id', 'lang']
                )
                
                if search_results.data:
                    print(f"Found {len(search_results.data)} tweets")
                    for tweet in search_results.data:
                        # Skip retweets
                        if hasattr(tweet, 'text') and tweet.text.startswith("RT @"):
                            continue
                            
                        tweet_data = {
                            'id': str(tweet.id),
                            'created_at': tweet.created_at.isoformat() if hasattr(tweet, 'created_at') else None,
                            'text': tweet.text if hasattr(tweet, 'text') else "",
                            'user_id': tweet.author_id if hasattr(tweet, 'author_id') else None,
                            'lang': tweet.lang if hasattr(tweet, 'lang') else None
                        }
                        
                        # Send to Kafka
                        producer.send(KAFKA_TOPIC, tweet_data)
                        print(f"Tweet sent to Kafka: {tweet_data['text'][:50]}...")
                else:
                    print("No tweets found in this search.")
                    
                # Wait longer between requests - Twitter Basic access is very limited
                print("Waiting 60 seconds before next request to avoid rate limits...")
                time.sleep(60)  # Increased to 60 seconds
                
            except tweepy.TooManyRequests:
                print("Rate limit exceeded - waiting for 15 minutes")
                time.sleep(15 * 60)  # 15 minutes
            except tweepy.TwitterServerError:
                print("Twitter server error - waiting for 2 minutes")
                time.sleep(2 * 60)  # 2 minutes
            except Exception as e:
                print(f"Error during search: {e}")
                time.sleep(60)  # Wait a minute if there's an error
                
    except KeyboardInterrupt:
        print("Stopping the producer...")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'producer' in locals():
            producer.close()

if __name__ == "__main__":
    main()