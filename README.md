# DSTI-Hadoop-Assignment
Using Scala to build a local Spark Streaming Application for Twitter data. Tweets are imported into an rdd. 

My analysis:
I am looking at topics that are experienced as shocking by the Twitter population. I am doing this by filtering incoming tweets on '#OMG', '#WTF' and '#SAYWHAT'. Then I do a wordcount, in order to find out which words are most associated with these. An interesting addition would have been to create a reference document of tweets and then compare the 'shocking' tweets to this reference, in order to get rid of some noise. Unfortunately I did not have time to implement this.
