package dsti

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache
import java.util.Calendar

import twitter4j.conf.ConfigurationBuilder
import org.apache.commons.configuration.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf

import org.apache.spark.rdd.RDD._
import org.apache.spark.SparkContext._



/**
  * Created by Joos Korstanje
  */
object Streamer {

  //The actual program!
  def main(args : Array[String]) {

    //This is only needed to solve a winutils bug.
    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    //Here I will set the account information of my Twitter app (existing of four different parts).
    //consumerKey
    val consumerKey = "/*secret*/"
    //"consumerSecret"
    val consumerSecret = "/*secret*/"
    //"accessToken"
    val accessToken = "/*secret*/"
    //"accessTokenSecret"
    val accessTokenSecret = "/*secret*/"


    //Here I give the values to the Configuration Builder and I set up the Configuraton Builder
    var bc = new conf.ConfigurationBuilder
    bc.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
    val auth = new OAuthAuthorization(bc.build)

    //Creating the Spark Configuration
    val config = new SparkConf().setMaster("local[2]").setAppName("twitter-app-joos")

    //Creating the Spark Context using the previously defined configuration details
    val sc = new SparkContext(config)

    //Creating the Spark Streaming Context
    val ssc = new StreamingContext(sc, Seconds(30))

    //Creating the stream of data that I want to obtain.
    //Project is: "Topics that are shocking the twitter population"
    var filters: Array[String]=new Array[String](10)

    filters(0) = "#OMG"
    filters(1) = "#omg"
    filters(2) = "#Omg"
    filters(3) = "#WTF"
    filters(4) = "#wtf"
    filters(5) = "#Wtf"
    filters(6) = "#saywhat"
    filters(7) = "#Saywhat"
    filters(8) = "#SAYWHAT"

    //Creating the Stream.
    val stream = TwitterUtils.createStream(ssc, Some(auth), filters).filter(_.getLang == "en")


    //Getting the tags in a file (two parts)
    //First: getting the texts of all the tweets
    val tags = stream.flatMap {status =>
      status.getHashtagEntities.map(_.getText)
    }

    //Second: getting only the tags.
    tags.countByValue()
            .map(x => (x, org.joda.time.DateTime.now()))
            .saveAsTextFiles("outfileytagsonly/")

    //Getting the stream
    val mydata = stream.map(l => l.getText()).saveAsTextFiles("mystream/")

    //Starting the stream
    //Leaving the Spark Streaming Context running by using an empty while loop.
    ssc.start()
    val t_end = System.currentTimeMillis()
    var t :Long =  0

    while (t < t_end + 100000) {
      t=System.currentTimeMillis()
    }

    //Stopping the Spark Streaming Context
    ssc.stop(false, true)


    //Creating an rdd from all the files in the Stream, organized per batch
    var mynewrdd = sc.textFile("mystream/*")

    //Mapping the rdd into duplets of wordcount (count, word) over all the collected tweets
    mynewrdd.flatMap(lines => lines.split(" "))
      .map(_.toLowerCase)
      .map(word => (word.replaceAll("""\W""", ""),1))
      .reduceByKey(_ + _)
      .map(_.swap)
      .coalesce(1)
      .sortByKey(false, 1)
      .saveAsTextFile("outfileztweets/")

  }
}