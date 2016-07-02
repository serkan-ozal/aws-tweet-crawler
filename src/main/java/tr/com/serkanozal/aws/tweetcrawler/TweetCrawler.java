package tr.com.serkanozal.aws.tweetcrawler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticsearch.AWSElasticsearchClient;
import com.amazonaws.services.elasticsearch.model.CreateElasticsearchDomainRequest;
import com.amazonaws.services.elasticsearch.model.CreateElasticsearchDomainResult;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainRequest;
import com.amazonaws.services.elasticsearch.model.DescribeElasticsearchDomainResult;
import com.amazonaws.services.elasticsearch.model.DomainInfo;
import com.amazonaws.services.elasticsearch.model.ElasticsearchDomainStatus;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesRequest;
import com.amazonaws.services.elasticsearch.model.ListDomainNamesResult;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetPolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
import com.amazonaws.services.kinesisfirehose.model.ElasticsearchBufferingHints;
import com.amazonaws.services.kinesisfirehose.model.ElasticsearchDestinationConfiguration;
import com.amazonaws.services.kinesisfirehose.model.ElasticsearchIndexRotationPeriod;
import com.amazonaws.services.kinesisfirehose.model.ElasticsearchS3BackupMode;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.ResourceNotFoundException;
import com.amazonaws.services.kinesisfirehose.model.S3DestinationConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

@SuppressWarnings("deprecation")
public class TweetCrawler {

    private static final Logger LOGGER = Logger.getLogger(TweetCrawler.class);
    
    private static final Properties configs;
    private static final AWSCredentials awsCredentials;

    static {
    	try {
    	    configs = getProperties("config.properties");
    	    
	        Properties awsProps = getProperties("aws-credentials.properties");
	        awsCredentials = 
	        		new BasicAWSCredentials(
	        				awsProps.getProperty("aws.accessKey"), 
	        				awsProps.getProperty("aws.secretKey"));
	        
    	} catch (IOException e) {
    		throw new RuntimeException(e);
    	}
    }
    
    public static void main(String[] args) throws TwitterException, IOException {        
        SearchInfo searchInfo = createTweetSearch();
        if (searchInfo != null) {
            LOGGER.info(searchInfo);
            LOGGER.info("You can access, analyze and visualize your search data through Kibana on " + 
                        "'" + searchInfo.searchDomainEndpoint + "/_plugin/kibana/" + "'");
            LOGGER.info("You can use 'twitter' as index name while configuring index pattern on Kibana dashboard");
        }
        
    	StreamInfo streamInfo = createTweetStream(searchInfo);
        if (streamInfo != null) {
            LOGGER.info(streamInfo);
        }
        
    	startTweetCrawling(streamInfo, searchInfo);
    }
    
    private static Properties getProperties(String propFileName) throws IOException {
    	Properties props = new Properties();
        try {
            InputStream in = TweetCrawler.class.getClassLoader().getResourceAsStream(propFileName);
            if (in != null) {
            	props.load(in);
            } 
            props.putAll(System.getProperties());
            return props;
        } catch (IOException e) {
            LOGGER.error("Error occured while loading properties from " + 
            			 "'" + propFileName + "'", e);
            throw e;
        }
    }
    
    private static String getContent(String fileName) {
        InputStream is = 
                TweetCrawler.class.getClassLoader().getResourceAsStream(fileName);
        Scanner scanner = new Scanner(is);  
        scanner.useDelimiter("\\Z");  
        return scanner.next(); 
    }
    
    private static class SearchInfo {
        
        private final String searchDomainName;
        private final String searchDomainEndpoint;
        private final String searchDomainARN;
        
        private SearchInfo(String searchDomainName, String searchDomainEndpoint, String searchDomainARN) {
            this.searchDomainName = searchDomainName;
            this.searchDomainEndpoint = searchDomainEndpoint;
            this.searchDomainARN = searchDomainARN;
        }

        @Override
        public String toString() {
            return "SearchInfo [searchDomainName=" + searchDomainName
                    + ", searchDomainEndpoint=" + searchDomainEndpoint
                    + ", searchDomainARN=" + searchDomainARN + "]";
        }

    }
    
    private static SearchInfo createTweetSearch() throws IOException {
        boolean tweetSearchEnable = Boolean.parseBoolean(configs.getProperty("config.tweetSearch.enable"));
        if (!tweetSearchEnable) {
            return null;
        }
        
        ////////////////////////////////////////////////////////////////
        
        Properties streamProps = getProperties("tweet-search.properties");
        String searchDomainName = streamProps.getProperty("aws.tweet.searchDomainName");

        ////////////////////////////////////////////////////////////////
        
        AmazonIdentityManagementClient iamClient = new AmazonIdentityManagementClient(awsCredentials);
        AWSElasticsearchClient elasticSearchClient = new AWSElasticsearchClient(awsCredentials);
        
        ////////////////////////////////////////////////////////////////
        
        ListDomainNamesRequest listDomainNamesRequest = new ListDomainNamesRequest();
        ListDomainNamesResult listDomainNamesResult = 
                elasticSearchClient.listDomainNames(listDomainNamesRequest);
        
        boolean skipSearchDomainCreation = false;
        String searchDomainEndpoint = null;
        String searchDomainARN = null;
        
        for (DomainInfo domainInfo : listDomainNamesResult.getDomainNames()) {
            if (searchDomainName.equals(domainInfo.getDomainName())) {
                skipSearchDomainCreation = true;
                break;
            }
        }
        
        ////////////////////////////////////////////////////////////////
        
        if (!skipSearchDomainCreation) {
            String accountId = iamClient.getUser().getUser().getUserId();
            String searchDomainPolicyDefinition = getContent("search-policy-definition-template");

            searchDomainPolicyDefinition = searchDomainPolicyDefinition.replace("${aws.user.accountId}", accountId);
            searchDomainPolicyDefinition = searchDomainPolicyDefinition.replace("${aws.tweet.searchDomainName}", searchDomainName);
            
            CreateElasticsearchDomainRequest createSearchDomainRequest = 
                    new CreateElasticsearchDomainRequest()
                            .withDomainName(searchDomainName)
                            .withAccessPolicies(searchDomainPolicyDefinition);
            CreateElasticsearchDomainResult elasticsearchDomainResult = 
                    elasticSearchClient.createElasticsearchDomain(createSearchDomainRequest);
            ElasticsearchDomainStatus domainStatus = elasticsearchDomainResult.getDomainStatus();
            searchDomainEndpoint = domainStatus.getEndpoint();
            searchDomainARN = domainStatus.getARN();
        }
        
        ////////////////////////////////////////////////////////////////
        
        while (true) {
            DescribeElasticsearchDomainRequest describeSearchDomainRequest =
                    new DescribeElasticsearchDomainRequest()
                            .withDomainName(searchDomainName);
            DescribeElasticsearchDomainResult describeSearchDomainResult =
                    elasticSearchClient.describeElasticsearchDomain(describeSearchDomainRequest);
            ElasticsearchDomainStatus domainStatus = describeSearchDomainResult.getDomainStatus();
            if (domainStatus.isCreated() && !domainStatus.isProcessing()) {
                searchDomainEndpoint = domainStatus.getEndpoint();
                searchDomainARN = domainStatus.getARN();
                break;
            }
            
            LOGGER.info("Waiting search domain " + searchDomainName + " to be activated ...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
        
        ////////////////////////////////////////////////////////////////

        return new SearchInfo(searchDomainName, searchDomainEndpoint, searchDomainARN);
    }     
    
    private static class StreamInfo {
        
        private final String streamName;
        private final String bucketName;
        
        private StreamInfo(String streamName, String bucketName) {
            this.streamName = streamName;
            this.bucketName = bucketName;
        }

        @Override
        public String toString() {
            return "StreamInfo [streamName=" + streamName + ", bucketName="
                    + bucketName + "]";
        }

    }
    
    private static StreamInfo createTweetStream(SearchInfo searchInfo) throws IOException {
        Properties streamProps = getProperties("tweet-stream.properties");

        String streamName = streamProps.getProperty("aws.tweet.streamName");
        String bucketName = streamProps.getProperty("aws.tweet.bucketName");
        Integer destinationSizeInMBs = Integer.parseInt(streamProps.getProperty("aws.tweet.bufferSize", "16")); // Default 16 MB
        Integer destinationIntervalInSeconds = Integer.parseInt(streamProps.getProperty("aws.tweet.bufferTime", "120")); // Default 2 mins
        String accountId = null;
        
        ////////////////////////////////////////////////////////////////
        
        AmazonIdentityManagementClient iamClient = new AmazonIdentityManagementClient(awsCredentials);
        AmazonS3Client s3Client = new AmazonS3Client(awsCredentials);
        AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials);
        
        ////////////////////////////////////////////////////////////////

        accountId = iamClient.getUser().getUser().getUserId();
        
        String roleName = "IAM_Role_for_" + streamName;
        boolean skipRoleCreation = false;
        try {
	        iamClient.getRole(new GetRoleRequest().withRoleName(roleName));
	        skipRoleCreation = true;
        } catch (NoSuchEntityException e) {
        }
        
        if (!skipRoleCreation) {
	        String roleDefinition = getContent("stream-role-definition-template"); 
	        roleDefinition = roleDefinition.replace("${aws.user.accountId}", accountId);
	
	        CreateRoleRequest createRoleRequest = new CreateRoleRequest();
	        createRoleRequest.setRoleName(roleName);
	        createRoleRequest.setPath("/");
	        createRoleRequest.setAssumeRolePolicyDocument(roleDefinition);
	        iamClient.createRole(createRoleRequest);
	        
	        LOGGER.info("Waiting role " + roleName + " to be activated ...");
	        try {
	         Thread.sleep(5000);
	        } catch (InterruptedException e) {
	        }
        }
        
        ////////////////////////////////////////////////////////////////
        
        String policyName = "Policy_for_" + roleName;
        String policyArn = "arn:aws:iam::" + accountId + ":policy/" + policyName;
        
        boolean skipPolicyCreation = false;
        try {
	        iamClient.getPolicy(new GetPolicyRequest().withPolicyArn(policyArn));
	        skipPolicyCreation = true;
        } catch (NoSuchEntityException e) {
        }
        
        if (!skipPolicyCreation) {
	        String policyDefinition = getContent("stream-policy-definition-template");
	        policyDefinition = policyDefinition.replace("${aws.tweet.bucketName}", bucketName);
	        policyDefinition = policyDefinition.replace("${aws.tweet.streamName}", streamName);
	        policyDefinition = policyDefinition.replace("${aws.user.accountId}", accountId);
	        if (searchInfo != null) {
	            policyDefinition = policyDefinition.replace("${aws.tweet.searchDomainName}", searchInfo.searchDomainName);
	        } else {
	            policyDefinition = policyDefinition.replace("${aws.tweet.searchDomainName}", "");
	        }
	        
	        CreatePolicyRequest createPolicyRequest = new CreatePolicyRequest();
	        createPolicyRequest.setPolicyName(policyName);
	        createPolicyRequest.setPolicyDocument(policyDefinition);
	        iamClient.createPolicy(createPolicyRequest);
	        
	        LOGGER.info("Waiting policy " + policyName + " to be activated ...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
        
        ////////////////////////////////////////////////////////////////
        
        boolean skipRolePolicyAttachment = false;
        try {
            iamClient.getRolePolicy(new GetRolePolicyRequest().withRoleName(roleName).withPolicyName(policyName));
            skipRolePolicyAttachment = true;
        } catch (NoSuchEntityException e) {
        }
        
        if (!skipRolePolicyAttachment) {
            AttachRolePolicyRequest attachRolePolicyRequest = new AttachRolePolicyRequest();
            attachRolePolicyRequest.setRoleName(roleName);
            attachRolePolicyRequest.setPolicyArn(policyArn);
            iamClient.attachRolePolicy(attachRolePolicyRequest);
            
            LOGGER.info("Waiting role-policy attachment to be activated ...");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
            }
        }
        
        ////////////////////////////////////////////////////////////////

        boolean skipStreamCreation = false;
        try {
	        firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest().withDeliveryStreamName(streamName));
	        skipStreamCreation = true;
        } catch (ResourceNotFoundException e) {
        }
        
        if (!skipStreamCreation) {
	        CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
	        createDeliveryStreamRequest.setDeliveryStreamName(streamName);
	        
	        if (!s3Client.doesBucketExist(bucketName)) {
                s3Client.createBucket(bucketName);
            }
            
            S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration();
            
            s3DestinationConfiguration.setBucketARN("arn:aws:s3:::" + bucketName);
            s3DestinationConfiguration.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
    
            BufferingHints bufferingHints = null;
            if (destinationSizeInMBs != null || destinationIntervalInSeconds != null) {
                bufferingHints = new BufferingHints();
                bufferingHints.setSizeInMBs(destinationSizeInMBs);
                bufferingHints.setIntervalInSeconds(destinationIntervalInSeconds);
            }
            s3DestinationConfiguration.setBufferingHints(bufferingHints);
    
            s3DestinationConfiguration.setRoleARN("arn:aws:iam::" + accountId + ":role/" + roleName);
    
            ////////////////////////////////////////////////////////////////
            
            if (searchInfo == null) {
                createDeliveryStreamRequest.setS3DestinationConfiguration(s3DestinationConfiguration);
            } else {
	            ElasticsearchDestinationConfiguration esDestinationConfiguration = new ElasticsearchDestinationConfiguration();

	            esDestinationConfiguration.setDomainARN(searchInfo.searchDomainARN);
	            esDestinationConfiguration.setIndexName("twitter");
	            esDestinationConfiguration.setTypeName("tweet");
	            esDestinationConfiguration.setS3BackupMode(ElasticsearchS3BackupMode.AllDocuments);
	            esDestinationConfiguration.setIndexRotationPeriod(ElasticsearchIndexRotationPeriod.NoRotation);
	            esDestinationConfiguration.setS3Configuration(s3DestinationConfiguration);
	            
                ElasticsearchBufferingHints esBufferingHints = null;
                if (destinationSizeInMBs != null || destinationIntervalInSeconds != null) {
                    esBufferingHints = new ElasticsearchBufferingHints();
                    esBufferingHints.setSizeInMBs(destinationSizeInMBs);
                    esBufferingHints.setIntervalInSeconds(destinationIntervalInSeconds);
                }
                esDestinationConfiguration.setBufferingHints(esBufferingHints);
                
                esDestinationConfiguration.setRoleARN("arn:aws:iam::" + accountId + ":role/" + roleName);

	            createDeliveryStreamRequest.setElasticsearchDestinationConfiguration(esDestinationConfiguration);
	        }
	        
	        firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
        }
        
        ////////////////////////////////////////////////////////////////

        return new StreamInfo(streamName, bucketName);
    }

    private static void startTweetCrawling(StreamInfo streamInfo, SearchInfo searchInfo) throws IOException {
    	AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials);
    	DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
    	describeDeliveryStreamRequest.setDeliveryStreamName(streamInfo.streamName);
    	
    	while (true) {
    		DescribeDeliveryStreamResult deliveryStreamResult = 
    				firehoseClient.describeDeliveryStream(describeDeliveryStreamRequest);
    		if ("ACTIVE".equals(deliveryStreamResult.getDeliveryStreamDescription().getDeliveryStreamStatus())) {
    			break;
    		}
    		LOGGER.info("Waiting stream " + streamInfo.streamName + " to be activated ...");
    		try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
    	}	
    	
        ////////////////////////////////////////////////////////////////
    	
        Properties twitterProps = getProperties("twitter-credentials.properties");
        
        ConfigurationBuilder twitterConf = new ConfigurationBuilder();
        twitterConf.setIncludeEntitiesEnabled(true);
        twitterConf.setOAuthAccessToken(twitterProps.getProperty("twitter.oauth.accessToken"));
        twitterConf.setOAuthAccessTokenSecret(twitterProps.getProperty("twitter.oauth.accessTokenSecret"));
        twitterConf.setOAuthConsumerKey(twitterProps.getProperty("twitter.oauth.consumerKey"));
        twitterConf.setOAuthConsumerSecret(twitterProps.getProperty("twitter.oauth.consumerSecret"));
        twitterConf.setJSONStoreEnabled(true);

        TwitterStream twitterStream = new TwitterStreamFactory(twitterConf.build()).getInstance();
        twitterStream.addListener(new TweetListener(firehoseClient, streamInfo.streamName));

        Properties filterProps = getProperties("tweet-filters.properties");
        
        String languagesValue = filterProps.getProperty("tweet.filter.languages");
        String[] languages = null;
        if (languagesValue != null) {
            languages = languagesValue.split(",");
        }
        
        String keywordsValue = filterProps.getProperty("tweet.filter.keywords");
        String[] keywords = null;
        if (keywordsValue != null) {
            keywords = keywordsValue.split(",");
        }
        
        String locationsValue = filterProps.getProperty("tweet.filter.locations");
        double[][] locations = null;
        if (locationsValue != null) {
            String[] locationsValueParts = locationsValue.split(",");
            locations = new double[locationsValueParts.length / 2][2];
            for (int i = 0; i < locations.length; i++) {
                locations[i] = new double[2];
                locations[i][0] = Double.parseDouble(locationsValueParts[i * 2]);
                locations[i][1] = Double.parseDouble(locationsValueParts[i * 2 + 1]);
            }
        } else {
            locations = new double[][] { { -180, -90 }, { 180, 90 } };
        }
        
        FilterQuery tweetFilterQuery = new FilterQuery();
        if (keywords != null && keywords.length > 0) {
            tweetFilterQuery.track(keywords);
        }
        if (languages != null && languages.length > 0) {
            tweetFilterQuery.language(languages);
        }
        tweetFilterQuery.locations(locations);
        twitterStream.filter(tweetFilterQuery);
    }
    
    private static class TweetListener implements StatusListener {

    	private final AmazonKinesisFirehoseClient firehoseClient;
    	private final String streamName;
    	
    	private TweetListener(AmazonKinesisFirehoseClient firehoseClient, String streamName) {
    		this.firehoseClient = firehoseClient;
    		this.streamName = streamName;
		}
    	
		@Override
        public void onStatus(Status status) {
		    if (LOGGER.isDebugEnabled()) {
		        LOGGER.debug("Tweet by @" + status.getUser().getScreenName() + ": " + status.getText());
		    }
            String tweetJsonData = DataObjectFactory.getRawJSON(status) + "\n";
            ByteBuffer tweetData = ByteBuffer.wrap(tweetJsonData.getBytes());
            PutRecordRequest putRecordRequest = 
                    new PutRecordRequest()
                        .withDeliveryStreamName(streamName)
                        .withRecord(new Record().withData(tweetData));
            firehoseClient.putRecord(putRecordRequest);
        }
        
        @Override
        public void onException(Exception ex) {
            LOGGER.error("On Exception!", ex);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        }

        @Override
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        }

        @Override
        public void onScrubGeo(long userId, long upToStatusId) {
        }

        @Override
        public void onStallWarning(StallWarning warning) {
        }
        
    }

}
