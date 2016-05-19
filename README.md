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
* **`aws.tweet.bufferSize:`** Size in MBs to buffer tweet data on AWS Firehose before uploading to AWS S3. Minimum value is `1MB`, maximum value is `128MB`. Default value is `64 MB`. 
* **`aws.tweet.bufferTime:`** Time in seconds to buffer tweet data on local before pushing to AWS S3. Minimum value is `60 seconds` (1 minute), maximum value is `900 seconds` (15 minutes). Default value is `600 seconds` (10 minutes).

These properties can be specified as system property or can be given from **`tweet-stream.properties`** configuration file.
`128MB`. 
2.4. Tweet Filter Configurations
--------------

* **`tweet.filter.languages:`** Interested languages to filter tweets. There can be multiple languages seperated by **comma** (`,`) but **without any space**. For example: `tr,en,fr`.
* **`tweet.filter.keywords:`** Interested keywords to filter tweets. There can be multiple keywords seperated by **comma** (`,`) but **without any space**. For example: `football,basketball,tennis`.
* **`tweet.filter.locations:`** Interested geographical region to filter tweets. Geographical points are given as `longitude,latitude` tupples. This means that given points are grouped as pair sequentially and first one is longitude value and the second one is latitude value. Each location point must be seperated by **comma** (`,`) but **without any space**. Default value is `[-180.0 longitude, -90.0 latitude] [+180.0 longitude, +90.0 latitude]` means all world. For example: `12.21,34.43,56.65,78.87`.

These properties can be specified as system property or can be given from **`tweet-filters.properties`** configuration file.

3. Deploy
==============
You can build and deploy the this Tweet crawler application to AWS via maven by `mvn -Pawseb clean package deploy` (or just run `build-and-deploy.sh`). So the crawler is packaged with its dependencies, uploaded to **AWS S3**, **AWS EC2** instance is provisioned and deployed onto it via **AWS Elastic Beanstalk**. Then it starts listening tweets and pushing them to **AWS S3** though **AWS Kinesis Firehose**.

**IMPORTANT NOTE:** Notice that the AWS Elastic Beanstalk's maven [plugin](http://beanstalker.ingenieux.com.br/beanstalk-maven-plugin/usage.html) needs `~/.aws/credentials` file that contains your AWS access and secret keys in the following format:

```
[default]
aws_access_key_id=XYZ123...
aws_secret_access_key=ABC567...
```

There are also some configurable properties in the `pom.xml`. Here are some of remarkable ones:
* **`beanstalk.instanceType:`** Specifies instance type of the AWS EC2 machine where crawler runs. See [here](https://aws.amazon.com/ec2/instance-types) for more details about AWS EC2 instance types.
* **`beanstalk.keyName:`** Specifies AWS EC2 key pair for connecting to the AWS EC2 machine where crawler runs.
* **`beanstalk.environmentType:`** Specifies whether crawler will run as a single instance or as a cluster behind the load balancer on multiple AWS EC2 instance with auto scaling options. In this project we are using `Single Instance` environment type because current logic (behaviour) of the application is not suitable for clustering and auto scaling because even though there are multiple applications on multiple instances, all of them will receive same tweets and so there will be no load distribution between the applications in the cluster. I have just mentioned this configuration to show that there is a AWS feature that  can be useful for other cases.
* **`aws:autoscaling.???:`** Specifies what is the limits of the cluster (minimum and maximum instance count in the cluster) for scaling up/down and in which conditions cluster will scale up/down.
