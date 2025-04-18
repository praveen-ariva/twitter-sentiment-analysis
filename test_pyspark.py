import tweepy

# Replace with your bearer token
bearer_token = "AAAAAAAAAAAAAAAAAAAAALLP0gEAAAAAVkQMdn7OoT1H96C1OpHucoI132c%3DOtfP1jFAkfUAv6HoysW92Oboiub91OzhumgsM4u5sybeEcknMU"

# Create a client with your bearer token
client = tweepy.Client(bearer_token=bearer_token)

# Try a simple search - this should work with Basic access
print("Testing Twitter API access...")
try:
    search_results = client.search_recent_tweets(query="python", max_results=10)
    print("✅ Search endpoint works!")
    if search_results.data:
        print(f"Found {len(search_results.data)} tweets")
        for tweet in search_results.data:
            print(f"Tweet: {tweet.text[:50]}...")
except Exception as e:
    print(f"❌ Search failed: {e}")