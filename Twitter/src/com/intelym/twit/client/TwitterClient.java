package com.intelym.twit.client;
import java.util.ArrayList;

import com.intelym.twit.service.TwitterService;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.TwitterException;

public class TwitterClient {
	 public static void main(String[] args) throws TwitterException {
		 TwitterService tInfo = new TwitterService();
		 
		//Search Twits by key
		ArrayList<Status> list = tInfo.getListOfTwits("AkshayMhasekar");
		for (Status tweet : list) {
			
			  System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
		}
		
		//Get list by page
		ArrayList<Status> Count = tInfo.getTwitsByPage(new Paging(1,2));
		for(Status tCount:Count){
				System.out.println(tCount.getText());
		}
			 
		 }
		 
	 }
		  
	        

