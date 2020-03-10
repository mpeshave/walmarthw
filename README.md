# walmarthw 

Data Pipeline

![Image description](pipeline.jpeg)

The project ingests live tweets using the Twitter Streaming API into a CouchbaseDb. The project is 
setup using the following tech:
1) Java 8
2) Spark 2.2.0
3) Couchbase 6.x
3) Twitter4j - open source repo providing a wrapper over Twitter Streaming API.

The data pipeline as seen in the image has 4 components:
1) Java application - uses the Twitter4j Streaming API to pull live tweets. The application writes it to a 
time based rolling file appender. The application creates a files for each min and puts its in a location 
specified with -Dlog.logBase JVM parameter. If -Dlog.logBase is not specified, the logs will be written to 
./logs/ director. The application also acts as a monitor, keeping count of number tweets received over http, 
number of unique tweets stored in CB and number of tweets received in a session.

2) 1 minute log files that are generated by the java application and are consumed by a spark streaming application.

3) Spark Streaming Application - Spark Structured Streaming application that consumes the 1 min log files
filters outs tweets related to music using basic filter function, repartitions the stream based on tweet id
so ensure duplicates are handled by same executor, theoratically. The transformed tweets are pushed into
couchbase. These mutations in couchbase as upserts to ensure duplicates are not added.

4) Couchbase store - holds tweet documents. The tweet id is used as the document id.

The data pipeline is split up into 2 working process.
1) Java Ingest app, to pull tweets and write to log files.
2) Spark Structured Streaming app to read these log files and push to couchbase.
Splitting up the data pipeline into such component will make it ease to monitor and maintain each component
separately without disturbing the other.

Build Process:
mvn clean package

Before running following must be setup:
1) Twitter dev account. Get consumer key, consumer secret, access token and access secret from twitter
site. These keys/tokens are to be used while running the data pipeline.
2)  A couchbase instance setup locally. 
3) Create a new bucket "twitter" in the local couchbase instance.

 
 