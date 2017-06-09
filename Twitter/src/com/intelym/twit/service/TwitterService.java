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

//public TwitterFactory tf; 
	TwitterFactory tf = null;
	static int x = 0;	
	
	public TwitterService(){
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		  .setOAuthConsumerKey("1nIbdh8pLNcmKSdktTRPpn4oi")
		  .setOAuthConsumerSecret("DybFYw5qLlVcXgm613ovZmHcircKSGK37lBVSb8DfxKZnMQwMI")
		  .setOAuthAccessToken("871983932182581248-nVwrXreKdhhSre91ce2RE9OQkOYFQBx")
		  .setOAuthAccessTokenSecret("oPXU9laqYVSJYtZyIL0fuF4kgDexWxQRkhIxMMndO1PAD");
		tf = new TwitterFactory(cb.build());
	}
		
	
	public ArrayList<Status> getListOfTwits(String key){
		Twitter twitter = tf.getInstance();
		ArrayList<Status> statusList = new ArrayList<Status>();

		  try {
	            Query query = new Query(key);
	            /*query.setSince("2017-06-07");
	            query.setUntil("2013-07-07");
	            query.geoCode(new GeoLocation(XXX, XXX), 200, Query.KILOMETERS);*/
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
	
	
	public ArrayList<Status> getTopTwits(Paging paging) throws TwitterException{
		Twitter twitter = tf.getInstance();
		
		//ResponseList<Status> a = twitter.getUserTimeline(new Paging(1,5));
		ArrayList<Status> statusList = new ArrayList<Status>();

		  try {
	            //Query query = new Query(key);
	          //  QueryResult result;
	          
        	ResponseList<Status> tweets = twitter.getUserTimeline(paging);
            //result = twitter.search(query);
           
            //List<Status> tweets = result.getTweets();
           
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
