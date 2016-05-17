# AWS Tweet Crawler

1. What is AWS Tweet Crawler?
==============
A Java project that craws Tweets via Twitter's streaming API then publishes and stores them on AWS. 

2. Configuration
==============

2.1. AWS Credentials
--------------
* **`aws.accessKey:`** Your AWS access key
* **`aws.secretKey:`** Your AWS secret key

These properties can be specified as system property or can be given from **`aws-credentials.properties`** configuration file.

2.2. Twitter Credentials
--------------

To use Twitter API, you need to have a registered application. If you have not, you can create it from [here](https://apps.twitter.com/).

* **`twitter.oauth.accessToken:`** Access token of your Twitter application
* **`twitter.oauth.accessTokenSecret:`** Access token secret of your Twitter application
* **`twitter.oauth.consumerKey:`** Consumer key of your Twitter application
* **`twitter.oauth.consumerSecret:`** Consumer secret of your Twitter application

These properties can be specified as system property or can be given from **`twitter-credentials.properties`** configuration file.

2.3. Tweet Stream Configurations
--------------

* **`aws.tweet.bucketName:`** Bucket name for AWS S3 to store tweet data
* **`aws.tweet.streamName:`** Stream name for AWS Kinesis Firehose pushing Tweet data
* **`aws.tweet.bufferSize:`** Size in MBs to buffer tweet data on local before pushing to AWS S3. Default value is `64 MB`.
* **`aws.tweet.bufferTime:`** Time in seconds to buffer tweet data on local before pushing to AWS S3. Default value is `900 seconds` (10 minutes).

These properties can be specified as system property or can be given from **`tweet-stream.properties`** configuration file.

3. Deploy
==============

TBD
