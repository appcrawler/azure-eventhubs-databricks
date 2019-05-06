// Databricks notebook source
import java.util._
import scala.collection.JavaConverters._
import com.microsoft.azure.eventhubs._
import java.util.concurrent._

import twitter4j._
import twitter4j.TwitterFactory
import twitter4j.Twitter
import twitter4j.conf.ConfigurationBuilder

val sasKeyName = "RootManageSharedAccessKey"
val sasKey = "********************"
val connStr = new ConnectionStringBuilder().setNamespaceName("sdhhubns").setEventHubName("sdhhub").setSasKeyName(sasKeyName).setSasKey(sasKey)

val pool = Executors.newFixedThreadPool(1)
val eventHubClient = EventHubClient.createSync(connStr.toString(), pool)

def sendEvent(message: String) = {
  val messageData = EventData.create(message.getBytes("UTF-8"))
  eventHubClient.sendSync(messageData);
  System.out.println("Sent tweet: " + message)
}

val twitterConsumerKey = "**********"
val twitterConsumerSecret = "***********************"
val twitterOauthAccessToken = "********************"
val twitterOauthTokenSecret = "*********************"

val cb = new ConfigurationBuilder()
cb.setDebugEnabled(true).setOAuthConsumerKey(twitterConsumerKey).setOAuthConsumerSecret(twitterConsumerSecret).setOAuthAccessToken(twitterOauthAccessToken).setOAuthAccessTokenSecret(twitterOauthTokenSecret)

val twitterFactory = new TwitterFactory(cb.build())
val twitter = twitterFactory.getInstance()

val query = new Query(" #kafka ")
query.setCount(100)
query.lang("en")
while (true) {
  val result = twitter.search(query)
  val statuses = result.getTweets()
  var lowestStatusId = Long.MaxValue
  for (status <- statuses.asScala) {
    if(!status.isRetweet()){
      sendEvent(status.getText())
    }
    lowestStatusId = Math.min(status.getId(), lowestStatusId)
    Thread.sleep(2000)
  }
  query.setMaxId(lowestStatusId - 1)
}

eventHubClient.close()

// COMMAND ----------


