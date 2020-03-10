package com.mpeshave.walmarthw

import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Spark streaming application to ingest minute log files containing tweets.
  * <p/>
  * The application filters out tweets that has a referrence to "music". This is done using a filter function
  * {@link TwitterStreamingIngest#filterFunction}.
  * <p/>
  * Application uses couchbase spark connector to stream data into couchbase. The couchbase spark connector
  * generates mutations to add data to bucket. These mutations are Upserts. If a document id is does not exists
  * a new doucment is added and if it already exists and if there are any changes the document will be updated.
  * This behaviour ensures that duplicate documents are not persisted.
  * <p/>
  * The stream is partitioned based on the tweet id so all tweets with the same id(range of ids) will end up in the same
  * partition and theorotically on the same executor.
  * <p/>
  * The stream application setup with a 2 second time interval to check and process new data.
  * <p/>
  * The application assumes the following are setup before running:
  * 1) Atleast 1 log file is generated and in the ingest read director the application is monitoring.
  * 2) Couchbase cluster running on localhost accessible on the default port.
  * 3) Couchbase bucket "twitter" created on the local couchbase cluster.
  * <p/>
  *
  * Application requires the following jvm params to be set:
  * 1) cb.username - couchbase username
  * 2) cb.passwd - couchbase pasword
  * 3) twitter.logbase - base directory where TwitterStreamingClient is writing log minute log files.
  *
  * These must be set when submitting spark application as extra jvm params to the driver with the following flag:
  * If logbase is not specified:
  *   --conf 'spark.driver.extraJavaOptions=-Dcb.username=$USERNAME -Dcb.passwd=$PASSWD'
  * If logbase is specified:
  *   --conf 'spark.driver.extraJavaOptions=-Dcb.username=$USERNAME -Dcb.passwd=$PASSWD -Dtwitter.logBase=$PATH'
  */
object TwitterStreamingIngest extends Serializable {
  def main(args: Array[String]): Unit = {

    var cbUsername: String = null
    var cbPasswd: String = null
    var logsBase: String = null

    cbUsername = System.getProperty("cb.username")
    cbPasswd = System.getProperty("cb.passwd")
    logsBase = System.getProperty("twitter.logsBase", "./logs/ingest")

    if (cbUsername == null || cbUsername.isEmpty) {
      throw new IllegalArgumentException("cb.username must not be null or empty")
    }

    if (cbPasswd == null || cbPasswd.isEmpty) {
      throw new IllegalArgumentException("cb.passwd must not be null or empty")
    }

    val spark = sparkSession(cbUsername, cbPasswd)
    val ds = inputStream(spark, logsBase)
    val ds1 = transform(ds, spark)
    val ds3 = writeStream(ds1)
    ds3.start().awaitTermination()
  }

  /**
    * Function to perform transformations on the input stream.
    * <p/>
    * The current implementation filters out tweets using the {@link filterFunction} and repartitions the
    * stream based on tweet id to ensure that tweets within a range end up in same partition.
    *
    * @param dataset
    * @param spark
    * @return
    */
  def transform(dataset: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    dataset
      .filter(filterFunction(_))
      .repartition($"id")
  }

  /**
    * Create a spark session.
    * <p/>
    * Ensure that couchbase configurations are also setup in the spark session.
    *
    * @return
    */
  def sparkSession(cbUsername: String, cbPasswd: String): SparkSession = {
    SparkSession.builder()//.master("local")
      .appName("TwitterStreaming")
      .config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on localhost
      .config("spark.couchbase.bucket.twitter", "")
      .config("com.couchbase.username", cbUsername)
      .config("com.couchbase.password", cbPasswd)
      //let the spark framework know to infer json schema from files.
      //TODO- if using json a defined schema must be used. Else use a a different file format, e.g: Avro.
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()
  }

  /**
    * Function that returns a {@code Dataset} that is hydrated by a stream.
    * <p/>
    * The current implementation
    * @param spark
    * @return
    */
  def inputStream(spark: SparkSession, logBase: String): DataFrame = {
    spark.readStream
      .option("wholeFile", true)
      .option("mode", "PERMISSIVE")
      .json( logBase)
  }

  /**
    * Function to write the {@code dataset} as a stream to desired sink.
    *
    * @param dataset
    */
  def writeStream(dataset: DataFrame): DataStreamWriter[Row] = {
    dataset.writeStream
      .option("checkpointLocation", "./sparkcheckpointlocation")
      .option("idField", "id")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .format("com.couchbase.spark.sql")
  }

  /**
    * Function filter out tweets with text containing a certain list of words.
    * <p/>
    * This is a very basic implementation on how to figure out tweets that have a reference to music.
    * The implementation can be more involved.
    * <p/>
    * This function can be modified as per filter requirements.
    *
    * @param row
    * @return
    */
  def filterFunction(row: Row): Boolean = {
    val keys = Seq("music", "band", "tour", "number", "genre", "radio", "audio", "audio", "song")
    val tweetText = row.getAs("text").toString.toLowerCase
    !keys.exists(tweetText.contains)
  }
}
