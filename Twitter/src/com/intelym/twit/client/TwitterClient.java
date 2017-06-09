package com.intelym.twit.client;
import java.util.ArrayList;

import com.intelym.twit.service.TwitterService;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.TwitterException;

public class TwitterClient {
	 public static void main(String[] args) throws TwitterException {
		 TwitterService tInfo = new TwitterService();
		 
		 ArrayList<Status> list = tInfo.getListOfTwits("AkshayMhasekar");
		
		 
		
		for (Status tweet : list) {
			
			  System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
	        }
		
		 ArrayList<Status> Count = tInfo.getTopTwits(new Paging(1,1));
		 
			for(Status tCount:Count){
					System.out.println(tCount.getText());
					
				}
			 
		 }
		 
	 }
		  
	        

