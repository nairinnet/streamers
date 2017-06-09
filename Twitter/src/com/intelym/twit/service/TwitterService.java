package com.intelym.twit.service;
import java.util.ArrayList;
import java.util.List;
import twitter4j.Paging;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
public class TwitterService {

	TwitterFactory tFactory = null;
	static int x = 0;	
	
	/**
	 * Initialize with configuration
	 */
	public TwitterService(){
		ConfigurationBuilder cBuilder = new ConfigurationBuilder();
		cBuilder.setDebugEnabled(true)
		  .setOAuthConsumerKey("*************************")
		  .setOAuthConsumerSecret("************************************")
		  .setOAuthAccessToken("********************************************")
		  .setOAuthAccessTokenSecret("****************************************");
		tFactory = new TwitterFactory(cBuilder.build());
	}
		
	/**
	 * Get List of Twits by key
	 * @param key
	 * @return
	 */
	public ArrayList<Status> getListOfTwits(String key){
		Twitter twitter = tFactory.getInstance();
		ArrayList<Status> statusList = new ArrayList<Status>();

		  try {
	            Query query = new Query(key);
	           
	            QueryResult result;
	           
	            do {
	            	
	                result = twitter.search(query);
	                
	                List<Status> tweets = result.getTweets();
	               
	                for (Status tweet : tweets) {
	                	statusList.add(tweet);
	                    
	                }
	              
	            } while ((query = result.nextQuery()) != null);
	            
	        } catch (TwitterException te) {
	            te.printStackTrace();
	            System.out.println("Failed to search tweets: " + te.getMessage());

	}
		  return statusList;
	}
	
	/**
	 * Get Twits by Page number and number of twits count
	 * @param paging
	 * @return
	 * @throws TwitterException
	 */
	public ArrayList<Status> getTwitsByPage(Paging paging) throws TwitterException{
		Twitter twitter = tFactory.getInstance();
		
		ArrayList<Status> statusList = new ArrayList<Status>();

		  try {
	          
        	ResponseList<Status> tweets = twitter.getUserTimeline(paging);
           
            for (Status tweet : tweets) {
            	statusList.add(tweet);
                
            }
	              
	        } catch (TwitterException te) {
	            te.printStackTrace();
	            System.out.println("Failed to search tweets: " + te.getMessage());
		}
		 return statusList;
	}
	 


}
