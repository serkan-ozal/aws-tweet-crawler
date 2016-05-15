package tr.com.serkanozal.aws.tweetcrawler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TweetCrawler {

    private static final Logger LOGGER = Logger.getLogger(TweetCrawler.class);
    
    public static void main(String[] args) throws TwitterException, IOException {
        // Twitter stream authentication setup
        Properties prop = (Properties) System.getProperties().clone();
        try {
            InputStream in = TweetCrawler.class.getClassLoader().getResourceAsStream("twitter-credentials.properties");
            if (in != null) {
                prop.load(in);
            } 
        } catch (IOException e) {
            LOGGER.error("Error occured while loading Twitter credentials from 'twitter-credentials.properties'", e);
        }
        
        // Set the configuration
        ConfigurationBuilder twitterConf = new ConfigurationBuilder();
        twitterConf.setIncludeEntitiesEnabled(true);
        twitterConf.setOAuthAccessToken(prop.getProperty("twitter.oauth.accessToken"));
        twitterConf.setOAuthAccessTokenSecret(prop.getProperty("twitter.oauth.accessTokenSecret"));
        twitterConf.setOAuthConsumerKey(prop.getProperty("twitter.oauth.consumerKey"));
        twitterConf.setOAuthConsumerSecret(prop.getProperty("twitter.oauth.consumerSecret"));
        twitterConf.setJSONStoreEnabled(true);

        // Create stream
        TwitterStream twitterStream = new TwitterStreamFactory(twitterConf.build()).getInstance();
        twitterStream.addListener(new TweetListener());

        // Setup filter
        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.language("tr");
        double[][] locations = { { -180, -90 }, { 180, 90 } };
        tweetFilterQuery.locations(locations);
        twitterStream.filter(tweetFilterQuery);
    }
    
    private static class TweetListener implements StatusListener {

        @Override
        public void onStatus(Status status) {
            LOGGER.info("Tweet by @" + status.getUser().getScreenName() + ": " + status.getText());
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
