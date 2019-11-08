# Twitter Data Streaming and Hashtag Counting

## Code Structure

twitterHTTPClient.py - Takes the Twitter API developer keys and opens a socket
that allows connections on a local port. The socket streams data directly from
Twitter using the "tweepy" library for Twitter API access. In this implementation,
the tags '#', 'content', and 'delivery' are tracked due to the API limitations
for standard accounts.

sparkStreaming.py - Connects to the twitterHTTPClient over a local port and begins
reading the streaming tweets. Puts all the hashtags into lower case and ensures
all of the following values are alpha-numeric. Apache Spark is used on the distributed
Dataproc cluster to aggregate the hashtags and sum all of the counts. The intermediate
results are stored in a bucket and the final results are written to a SQL database.
The streaming time is currently set to 20 minutes, but can easily be adjusted with the
STREAMTIME variable.

## Data Visualization

A Google Colab notebook is written for each run of the above software. The BigQuery
hashtag count table is pulled into a Pandas dataframe and sorted by count in a
descending order. The top ten hashtag values are selected and saved into a new
dataframe. The Plotly python library is used to generate donut charts and bar charts
for the data.

The goal is to use the frequency distribution of the hashtags as a surrogate popularity
distribution for all of the tweets on Twitter. This provides a means to make caching
recommendations for Twitter to allow for fast lookups of tweets containing heavily-accessed 
hashtags.

## Notes

Both of the programs run on a Google Cloud Dataproc cluster. This implementation
also relies on Google BigQuery as the SQL database to store the hashtag counts
after program completion. Additionally, a Google Storage bucket is required to
hold the intermediate execution results.