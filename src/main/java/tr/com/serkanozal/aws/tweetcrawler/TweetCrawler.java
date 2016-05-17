package tr.com.serkanozal.aws.tweetcrawler;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClient;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyResult;
import com.amazonaws.services.identitymanagement.model.CreateRoleRequest;
import com.amazonaws.services.identitymanagement.model.GetPolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetRoleRequest;
import com.amazonaws.services.identitymanagement.model.NoSuchEntityException;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.BufferingHints;
import com.amazonaws.services.kinesisfirehose.model.CompressionFormat;
import com.amazonaws.services.kinesisfirehose.model.CreateDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamRequest;
import com.amazonaws.services.kinesisfirehose.model.DescribeDeliveryStreamResult;
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
    
    private static final AWSCredentials awsCredentials;
    
    static {
    	try {
	    	// Twitter stream authentication setup
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
    	String streamName = createTweetStream();
    	
    	startTweetCrawling(streamName);
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
    
    private static String createTweetStream() throws IOException {
        // Twitter stream configuration setup
        Properties streamProps = getProperties("tweet-stream.properties");

        String streamName = streamProps.getProperty("aws.tweet.streamName");
        String bucketName = streamProps.getProperty("aws.tweet.bucketName");
        Integer destinationSizeInMBs = Integer.parseInt(streamProps.getProperty("aws.tweet.bufferSize", "64")); // Default 64 MB
        Integer destinationIntervalInSeconds = Integer.parseInt(streamProps.getProperty("aws.tweet.bufferTime", "600")); // Default 10 mins
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
	        String roleDefinition = null; 
	        InputStream inRole = TweetCrawler.class.getClassLoader().getResourceAsStream("role-definition-template");
	        Scanner scanRole = new Scanner(inRole);  
	        scanRole.useDelimiter("\\Z");  
	        roleDefinition = scanRole.next(); 
	
	        CreateRoleRequest createRoleRequest = new CreateRoleRequest();
	        createRoleRequest.setRoleName(roleName);
	        createRoleRequest.setPath("/");
	        createRoleRequest.setAssumeRolePolicyDocument(roleDefinition);
	        iamClient.createRole(createRoleRequest);
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
	        String policyDefinition = null; 
	        InputStream inPolicy = TweetCrawler.class.getClassLoader().getResourceAsStream("policy-definition-template");
	        Scanner scanPolicy = new Scanner(inPolicy);  
	        scanPolicy.useDelimiter("\\Z");  
	        policyDefinition = scanPolicy.next(); 
	        
	        policyDefinition = policyDefinition.replace("${aws.tweet.bucketName}", bucketName);
	        policyDefinition = policyDefinition.replace("${aws.tweet.streamName}", streamName);
	        
	        CreatePolicyRequest createPolicyRequest = new CreatePolicyRequest();
	        createPolicyRequest.setPolicyName(policyName);
	        createPolicyRequest.setPolicyDocument(policyDefinition);
	        CreatePolicyResult createPolicyResult = iamClient.createPolicy(createPolicyRequest);
	        
	        AttachRolePolicyRequest attachRolePolicyRequest = new AttachRolePolicyRequest();
	        attachRolePolicyRequest.setRoleName(roleName);
	        attachRolePolicyRequest.setPolicyArn(createPolicyResult.getPolicy().getArn());
	        iamClient.attachRolePolicy(attachRolePolicyRequest);
	        
	        try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
        }
        
        ////////////////////////////////////////////////////////////////

        if (!s3Client.doesBucketExist(bucketName)) {
        	s3Client.createBucket(bucketName);
        }
        
        ////////////////////////////////////////////////////////////////

        boolean skipStreamCreation = false;
        try {
	        firehoseClient.describeDeliveryStream(new DescribeDeliveryStreamRequest().withDeliveryStreamName(streamName));
	        skipStreamCreation = true;
        } catch (ResourceNotFoundException e) {
        }
        
        if (!skipStreamCreation) {
	        // Create deliveryStream
	        CreateDeliveryStreamRequest createDeliveryStreamRequest = new CreateDeliveryStreamRequest();
	        createDeliveryStreamRequest.setDeliveryStreamName(streamName);
	
	        S3DestinationConfiguration s3DestinationConfiguration = new S3DestinationConfiguration();
	        s3DestinationConfiguration.setBucketARN("arn:aws:s3:::" + bucketName);
	        // Could also specify GZIP, ZIP, or SNAPPY
	        s3DestinationConfiguration.setCompressionFormat(CompressionFormat.UNCOMPRESSED);
	
	        BufferingHints bufferingHints = null;
	        if (destinationSizeInMBs != null || destinationIntervalInSeconds != null) {
	            bufferingHints = new BufferingHints();
	            bufferingHints.setSizeInMBs(destinationSizeInMBs);
	            bufferingHints.setIntervalInSeconds(destinationIntervalInSeconds);
	        }
	        s3DestinationConfiguration.setBufferingHints(bufferingHints);
	
	        // Create and set the IAM role so that Firehose has access to the S3 buckets to put data
	        // and AWS KMS keys (if provided) to encrypt data. Please check the trustPolicyDocument.json and
	        // permissionsPolicyDocument.json files for the trust and permissions policies set for the role.
	        s3DestinationConfiguration.setRoleARN("arn:aws:iam::" + accountId + ":role/" + roleName);
	
	        createDeliveryStreamRequest.setS3DestinationConfiguration(s3DestinationConfiguration);
	
	        firehoseClient.createDeliveryStream(createDeliveryStreamRequest);
        }
        
        ////////////////////////////////////////////////////////////////

        return streamName;
    }
    
    private static void startTweetCrawling(String streamName) throws IOException {
    	AmazonKinesisFirehoseClient firehoseClient = new AmazonKinesisFirehoseClient(awsCredentials);
    	DescribeDeliveryStreamRequest describeDeliveryStreamRequest = new DescribeDeliveryStreamRequest();
    	describeDeliveryStreamRequest.setDeliveryStreamName(streamName);
    	
    	while (true) {
    		DescribeDeliveryStreamResult deliveryStreamResult = 
    				firehoseClient.describeDeliveryStream(describeDeliveryStreamRequest);
    		if ("ACTIVE".equals(deliveryStreamResult.getDeliveryStreamDescription().getDeliveryStreamStatus())) {
    			break;
    		}
    		LOGGER.info("Waiting stream " + streamName + " to be activated ...");
    		try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
    	}	
    	
        ////////////////////////////////////////////////////////////////
    	
        // Twitter stream authentication configurations
        Properties twitterProps = getProperties("twitter-credentials.properties");
        
        // Set the configuration
        ConfigurationBuilder twitterConf = new ConfigurationBuilder();
        twitterConf.setIncludeEntitiesEnabled(true);
        twitterConf.setOAuthAccessToken(twitterProps.getProperty("twitter.oauth.accessToken"));
        twitterConf.setOAuthAccessTokenSecret(twitterProps.getProperty("twitter.oauth.accessTokenSecret"));
        twitterConf.setOAuthConsumerKey(twitterProps.getProperty("twitter.oauth.consumerKey"));
        twitterConf.setOAuthConsumerSecret(twitterProps.getProperty("twitter.oauth.consumerSecret"));
        twitterConf.setJSONStoreEnabled(true);

        // Create stream
        TwitterStream twitterStream = new TwitterStreamFactory(twitterConf.build()).getInstance();
        twitterStream.addListener(new TweetListener(firehoseClient, streamName));

        // Twitter filter configurations
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
        
        // Setup filter
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
            String jsonData = DataObjectFactory.getRawJSON(status);
            ByteBuffer data = ByteBuffer.wrap(jsonData.getBytes());
            PutRecordRequest putRecordRequest = 
                    new PutRecordRequest()
                        .withDeliveryStreamName(streamName)
                        .withRecord(new Record().withData(data));
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
