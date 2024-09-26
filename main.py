import pandas as pd
import sqlite3
from datetime import datetime
class PotatoTakeHome():
    def __init__(self):
        pass
    def IngestingData(self):
        # Reading TSV File from Project directory path
        file_path = "correct_twitter_201904.tsv"
        tweets_df = pd.read_csv(file_path, sep='\t')
        # Display the first few rows to understand structure
        print(tweets_df.head())
        tweets_df.to_csv("correct_twitter_201904.csv")
        # Store data in SQLite for efficient querying
        conn = sqlite3.connect('tweets_data.db')
        tweets_df.to_sql('tweets', conn, if_exists='replace', index=False)
        # Close sql connection
        conn.close()
    def QueryingData(self, term=''):
        # Connect to the SQLite DB
        conn = sqlite3.connect('tweets_data.db')
        cursor = conn.cursor()
        # Query to get tweets containing the term
        retrieve_query = f"""SELECT created_at, user_id, tweet_text, place_id, favorite_count, created_at FROM tweets 
                            WHERE tweet_text LIKE '%{term}%' """
        cursor.execute(retrieve_query)
        results = cursor.fetchall()
        # Post-processing to aggregate results
        tweet_count = len(results)
        unique_users = len(set([row[1] for row in results]))
        avg_likes = sum([row[4] for row in results]) / tweet_count if tweet_count > 0 else 0
        most_active_user = max(set([row[1] for row in results]), key=[row[1] for row in results].count)
        # Grouping by date for tweet counts
        daily_tweets = {}
        for row in results:
            tweet_date = datetime.strptime(row[0], "%Y-%m-%d").date()
            if tweet_date in daily_tweets:
                daily_tweets[tweet_date] += 1
            else:
                daily_tweets[tweet_date] = 1
        # Return the aggregated results
        return {
            'tweet_count': tweet_count,
            'unique_users': unique_users,
            'avg_likes': avg_likes,
            'daily_tweets': daily_tweets,
            'most_active_user': most_active_user,
        }
        # Close connection
        conn.close()

if __name__=="__main__":
    mainclassrun = PotatoTakeHome()
    mainclassrun.IngestingData()
    mainclassrun.QueryingData(term='music')

"""
1. Docker Integration:
    - I will create a Dockerfile to containerize the system for easy setup across different environments.
2. NoSQL:
    - I will provide a MongoDB integration option to store and query data using NoSQL, improving flexibility.
3. Flask API:
    - I'll create a REST API using Flask for querying the system remotely, which can then be deployed as a microservice.
4. Optimizations:
    - Indexing and query optimizations for faster lookups.
    - Optionally, we can use Elasticsearch to improve the speed of full-text search.
5. Testing:
    - I will implement tests using pytest to ensure the query functions return accurate results.
I'll now proceed to implement the detailed solution and push the code to a Github repository as per the instructions.
If you need any more specific details or adjustments, feel free to ask!"""