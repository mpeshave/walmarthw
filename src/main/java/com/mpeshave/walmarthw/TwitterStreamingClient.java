package com.mpeshave.walmarthw;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.util.function.Consumer;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link TwitterStream} client to connect to twitter streaming api service to receive and log new tweets over
 * HTTP live connection.
 * <p/>
 * This client implementation has 3 threads running, one each for the following tasks:
 * 1) Thread to run the Twitter4j streaming client that keeps listening on HTTP for new tweets and pushes the new tweets
 *    to a blocking queue.
 * 2) Thread to monitor the queue for new messages and logs these messages to a time based rolling file append logger.
 * 3) Thread that runs as a timed tasks every 30 second to monitor the overall progress. Prints counters for different
 *    stages of the pipeline.
 * <p/>
 * The application assumes the following are setup:
 * 1) Twitter developer account creds and tokens. These must be passed as jvm params.
 * 2) Couchbase cluster running on localhost accessible on default port.
 * 3) Couchbase bucket "twitter" created on the local cluster.
 * 4) ./logs/ingest directory created for the application to move rolling log files to that folder.
 *
 * The application mandates the following jvm parameters are set:
 * 1) cb.username - couchbase username
 * 2) cb.passwd - couchbase passwd
 * 3) log4j.logBase - This is optional. twitter ingest log base
 * 4) twitter.consumerKey - OAuth Consumer Key
 * 5) twitter.consumerSecret - OAuth Consumer Secret
 * 6) twitter.accessToken - OAuth access token
 * 7) twitter.accessSecret - OAuth access secret
 */
public class TwitterStreamingClient {
    /**
     * Counter to keep count of the total tweets received since the start. This count includes duplicate tweets as well
     * count of tweets that are filtered out through out the pipeline.
     */
    public volatile AtomicInteger totalCounter;

    /**
     * Keeps a count of the number of tweets received during the current run of the {@link TwitterStream} client.
     */
    public int sessionCounter;

    /**
     * Time based {@link org.apache.log4j.RollingFileAppender} logger to log tweets received to a min based log file.
     * <p/>
     * The logger is setup to roll over to a new log file ever minute. This can be increated to create 5 min or 10 min
     * files.
     * <p/>
     * The logger configuration is setup in resources/something.properties file.
     */
    public final Logger logger = org.slf4j.LoggerFactory.getLogger(TwitterStreamingClient.class);

    /**
     * Blocking queue used as a buffer to hold new incoming tweets before they are writtern to log file by a separate thread.
     */
    public final BlockingQueue<String> messageQueue;

    /**
     * Executor service to run runnable tasks submitted.
     */
    public final ExecutorService executorService;

    /**
     * Couchbase cluster used to store counters.
     */
    public final Cluster cluster;

    /**
     * Couch base bucket to store counter.
     */
    public final Bucket bucket;

    /**
     * Twitter account access keys and tokens to be used for API access.
     */
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessSecret;

    public static Logger rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);

    public TwitterStreamingClient(String cbUsername, String cbPasswd,
                                  String consumerKey, String consumerSecret,
                                  String accessToken, String accessSecret) {
        this.sessionCounter = 0;
        this.totalCounter = new AtomicInteger();
        this.messageQueue = new LinkedBlockingQueue<String>();
        this.executorService = Executors.newFixedThreadPool(2);

        assert cbUsername != null || !cbUsername.isEmpty();
        assert cbPasswd != null || !cbPasswd.isEmpty();

        //TODO- close the cluster connection properly in the cleanup method.
        //The client will try to connect to a couchbase cluster on local host.
        this.cluster = CouchbaseCluster.create("localhost");
        // Cluster credentials, must be passed as jvm variables.
        this.cluster.authenticate(cbUsername,cbPasswd);
        //"twitter" bucket is expected to be created in the local couchbase cluster before running this client.
        this.bucket = cluster.openBucket("twitter");

        assert consumerKey != null || !consumerKey.isEmpty();
        assert consumerSecret != null || !consumerSecret.isEmpty();
        assert accessToken != null || !accessToken.isEmpty();
        assert accessSecret != null || !accessSecret.isEmpty();

        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessSecret = accessSecret;
    }

    public static void main(String[] args) {
        String cbUsername;
        String cbPasswd;
        String consumerKey;
        String consumerSecret;
        String accessToken;
        String accessSecret;

        try {

            cbUsername = System.getProperty("cb.username");
            cbPasswd = System.getProperty("cb.passwd");

            consumerKey = System.getProperty("twitter.consumerKey");
            consumerSecret = System.getProperty("twitter.consumerSecret");
            accessToken = System.getProperty("twitter.accessToken");
            accessSecret = System.getProperty("twitter.accessSecret");

            if (cbUsername == null || cbUsername.isEmpty()) {
                //throw new IllegalArgumentException("cb.username must not be null or empty");
                System.out.println("cb.username must not be null or empty");
                System.exit(-1);
            }

            if (cbPasswd == null || cbPasswd.isEmpty()) {
                //throw new IllegalArgumentException("cb.passwd must not be null or empty");
                System.out.println("cb.passwd must not be null or empty");
                System.exit(-1);
            }

            if (consumerKey == null || consumerKey.isEmpty()) {
                //throw new IllegalArgumentException("twitter.consumerKey must not be null or empty");
                System.out.println("twitter.consumerKey must not be null or empty");
                System.exit(-1);
            }

            if (consumerSecret == null || consumerSecret.isEmpty()) {
                //throw new IllegalArgumentException("twitter.consumerSecret must not be null or empty");
                System.out.println("twitter.consumerSecret must not be null or empty");
                System.exit(-1);
            }

            if (accessToken == null || accessToken.isEmpty()) {
                //throw new IllegalArgumentException("twitter.accessToken must not be null or empty");
                System.out.println("twitter.accessToken must not be null or empty");
                System.exit(-1);
            }

            if (accessSecret == null || accessSecret.isEmpty()) {
                //throw new IllegalArgumentException("twitter.accessSecret must not be null or empty");
                System.out.println("twitter.accessSecret must not be null or empty");
                System.exit(-1);
            }

            System.out.println("Couchbase username: " + cbUsername);
            System.out.println("Couchbase passwd: " + cbPasswd);
            System.out.println("Consumer Key: " + consumerKey);
            System.out.println("Consumer Secrent: " + consumerSecret);
            System.out.println("Access Token: " + accessToken);
            System.out.println("Access Secret: " + accessSecret);
            System.out.println("------------------------------------------------");

            new TwitterStreamingClient(cbUsername, cbPasswd, consumerKey, consumerSecret, accessToken, accessSecret).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
        } finally {
            //TODO - perform clean up before exiting.
        }
    }

    /**
     * Method to initialize and start the execution of different threads in an order.
     * <p/>
     * As mentioned above, 3 threads are started.
     * 1) Twitter4J http listener thread.
     * 2) Blocking Queue monitoring thread.
     * 3) Timer task thread to monitor the progress.
     */
    public void execute() {
        //Counter initialization and logging.
        JsonLongDocument jsonLongDocument = this.bucket.counter("consumed_raw_tweets", 0, 0);
        System.out.println(String.format("Total Tweets received(over http by twitter4j client): %d", jsonLongDocument.content().intValue()));
        System.out.println(String.format("Unique Tweets Consumed(processed and stored in CB): %d", getTotalUniquesIngested()));
        System.out.println(String.format("Tweets Consumed Current Session(received over http by twitter4j client): %d", sessionCounter));

        //Add a task to run twitter4j streaming client and add new tweets to blocking queue.
        executorService.submit(getTwitter4jRunnerThread());

        //add task to monitor blocking queue for new tweets and log them to minute based rolling log files.
        executorService.submit(getQueueMonitoringThread());

        //Timer task to poll and print counters every 30 seconds.
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                JsonLongDocument jsonLongDocument = bucket.counter("consumed_raw_tweets", 0, 0);
                bucket.counter("consumed_raw_tweets", totalCounter.get());

                try {
                    Runtime.getRuntime().exec("clear");
                } catch (IOException e) {

                }
                System.out.println("------------------------------------------------------------");
                System.out.println(String.format("Total Tweets Consumed(over http by twitter4j client): %d", (jsonLongDocument.content().intValue() + totalCounter.get())));
                System.out.println(String.format("Unique Tweets Consumed(processed and stored in CB): %d", getTotalUniquesIngested()));
                System.out.println(String.format("Tweets Consumed Current Session(received over http by twitter4j client): %d", sessionCounter));
                totalCounter.set(0);

            }
        }, 30000, 30000);
    }

    /**
     * Returns a {#code Runnable} that runs and instance of {@link TwitterStream}.
     * <p/>
     * The {@link TwitterStreamingClient} keeps a HTTP connection alive and listens for new tweets.
     * <p/>
     * The current implementation uses a STANDARD version of the API. The standard version is limited to
     * using only a single connection from the same IP.
     *
     * @return {@link Runnable}
     */
    public Runnable getTwitter4jRunnerThread() {

        return new Runnable() {
            @Override
            public void run() {
                ConfigurationBuilder cb = new ConfigurationBuilder();
                cb.setDebugEnabled(true)
                        .setOAuthConsumerKey(consumerKey)
                        .setOAuthConsumerSecret(consumerSecret)
                        .setOAuthAccessToken(accessToken)
                        .setOAuthAccessTokenSecret(accessSecret)
                        .setJSONStoreEnabled(true);

                try {
                    TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
                    twitterStream.onStatus(new Consumer<Status>() {
                        @Override
                        public void accept(Status status) {
                            if (status != null) {
                                String statusJson = TwitterObjectFactory.getRawJSON(status);
                                messageQueue.add(statusJson);
                            }
                        }
                    });
                    twitterStream.onException(new Consumer<Exception>() {
                        @Override
                        public void accept(Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    });

                    //Filter to consume only the tweets containing "justin bieber" string combinations.
                    twitterStream.filter(new FilterQuery()
                            .track(new String[]{"justin bieber","Justin Bieber","JustinBieber","JUSTINBBIEBER", "justinbieber"}));
                } catch (Exception e) {
                    //e.printStackTrace();
                    System.out.println(e.getMessage());
                    System.exit(-1);
                }
            }
        };
    }

    /**
     * Returns an instance of {@link Runnable} that monitors the {@link TwitterStreamingClient#messageQueue} and log
     * new messages added in the queue to a time based {@link org.apache.log4j.RollingFileAppender} logger
     * {@link TwitterStreamingClient#logger}
     *
     * @return {@link Runnable}
     */
    public Runnable getQueueMonitoringThread() {
        return new Runnable() {
            @Override
            public void run() {
                //TODO - keep this thread alive based on a keep alive flag, so as to cleanup in a better way later.
                while(true) {
                    if(!messageQueue.isEmpty()) {
                        String str = messageQueue.poll();
                        //Log new tweet to a minute log file.
                        logger.info(str.trim());
                        totalCounter.incrementAndGet();
                        sessionCounter+=1;
                    }
                }
            }
        };
    }

    /**
     * Returns a total number of tweet documents added to couchbase.
     * <p/>
     * This is a unique count as the spark streaming job document inserts are actually upserts.
     *
     * @return Total unique counts of tweet documents ingested into the store.
     */
    public int getTotalUniquesIngested() {
        JsonObject json = this.bucket.bucketManager().info().raw();
        return json.getObject("basicStats").getInt("itemCount");
    }
}
