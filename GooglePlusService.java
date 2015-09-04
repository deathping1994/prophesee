package com.prophesee.datafetcher.bl.media_platform_services;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonNode;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
//import org.mongodb.mongo_java_driver.DB;			// don't know why was it included but just try uncommenting when connected to internet
//import org.mongodb.mongo_java_driver.MongoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.prophesee.datafetcher.bl.ApplicationInitializer;
import com.prophesee.datafetcher.bl.MediaPlatformService;
import com.prophesee.datafetcher.bl.callables.FetchDataCallable;
import com.prophesee.datafetcher.bl.db_services.ElasticsearchService;
import com.prophesee.datafetcher.bl.media_platform_services.YoutubeService;
import com.prophesee.datafetcher.enums.Duration;
import com.prophesee.datafetcher.enums.MediaPlatform;
import com.prophesee.datafetcher.exceptions.JSONParamsException;
import com.prophesee.datafetcher.exceptions.JobExecutionFailedException;
import com.prophesee.datafetcher.util.JSONOperations;
import com.prophesee.datafetcher.util.StringConstants;

public class GooglePlusService implements MediaPlatformService {

	private static final Logger logger = LoggerFactory.getLogger(GooglePlusService.class);
	
	public static MongoClient mongoClient = ApplicationInitializer.mongoClient;
	
	public static DBCollection googleplusCollection;
	public DBCollection userCollection;	
	private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	
	private static GoogleCredential credentials;
	
	
	public GooglePlusService(){
		googleplusCollection = ApplicationInitializer.propheseeDB.getCollection("googleplus");
		this.userCollection= ApplicationInitializer.propheseeDB.getCollection("user2");
		String clientId = "714917954023-hl30snccjkpj6tbb4vr6k906v8nek3mb.apps.googleusercontent.com";
		String clientSecret = "D0LJ6RO7kAYggktxKucoz3li";
	
		credentials = new GoogleCredential.Builder()
		.setTransport(HTTP_TRANSPORT)
		.setJsonFactory(JSON_FACTORY)
		.setClientSecrets(clientId,clientSecret)
		.build();
		}
	
	
	@Override
	public void startFetch() {

		BasicDBObject query = new BasicDBObject("gplus_Id", new BasicDBObject("$exists", "true"));
		
		DBCursor cursor = userCollection.find(query);
		
		try {
		   while(cursor.hasNext()) {
			   
			   DBObject user = cursor.next();
			   
			   fetchPublicData("Google Plus",user.get("gplus_Id").toString());

		  }
		} finally {
		  cursor.close();
		} 
		
	}

	@Override
	public void startFetchPostsData(Duration duration) {
		// TODO Auto-generated method stub
		
		System.out.println("Inside StartFetch Google Plus Activity Data");
			
			BasicDBObject query = new BasicDBObject("gplus_Id", new BasicDBObject("$exists", "true"));
			
			DBCursor cursor = userCollection.find(query);			
					
			ExecutorService executor = Executors.newFixedThreadPool(5);

			try {
			   while(cursor.hasNext()) {
				   
				   DBObject user = cursor.next();
				  
				  executor.submit(
					   new FetchDataCallable(
								   this,
								   MediaPlatform.GOOGLE_PLUS,
								   user.get("gplus_Id").toString(),
								   user.get("gplus_Id").toString(),									
								   duration
						   )
					);
				   
				   //fetchPostData(user.get("gplusId").toString(),user.get("gplusId").toString(),Duration.ONE_DAY);
			   }
			} finally {
			   cursor.close();
			}
			
			executor.shutdown();
			
			while(!executor.isTerminated())
				;
			
			System.out.println("\n\n\n Google Plus Data Fetch Executor has been shutdown successfully!\n\n\n");
		}

	

	
	@Override
	public void fetchPublicData(String mediaPlatformId, String brandId) {
		

System.out.println("Fetching Google Plus public data for Brand: "+ brandId);
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.INSIDE_METHOD);
		
		/*
		 * Create and instantiate HTTP call objects
		 */
		HttpURLConnection connection = null;
		String responseData = null;
		String jobUrl = "https://www.googleapis.com/plus/v1/people/"
						+brandId
						+"?fields=displayName,circledByCount,objectType,plusOneCount,image/url&key=AIzaSyDCyoS7wsxZdSRCmUozarU435it8zJ5oaI";
		try {
			
			URL url = new URL(jobUrl);		
		    connection = (HttpURLConnection) url.openConnection();
		    connection.setRequestMethod("GET");
		    connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
		    
		    InputStream responseStream = connection.getInputStream();
		    String charset = "UTF-8";
		    /*
		     * Fetching response data for the HTTP request
		     */ 
		    responseData = IOUtils.toString(responseStream, charset);		
		    storePublicData(responseData, brandId);
		
	} catch (Exception e) {
			/*
			 * Exception caught while hitting the URL
			 * Error executing job.
			 */
		if(logger.isErrorEnabled())
		logger.error(StringConstants.ERROR_PERFORMING_URL_CALL + jobUrl, e);
			
			try {
				throw new JobExecutionFailedException(StringConstants.JOB_EXECUTION_FAILED + jobUrl, e);
			} catch (JobExecutionFailedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.INSIDE_METHOD);
		

	}

	@Override
	public Map<?, ?> getPublicData(String brandId) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void storePublicData(String data, String brandId) {
		
		
		try {
			

			JsonNode publicDataJsonNode = JSONOperations.castFromStringToJSONNode(data);
			System.out.println(publicDataJsonNode);
			Date day = new Date(DateTime.now(DateTimeZone.UTC).withTimeAtStartOfDay().getMillis());

			BasicDBObject query = new BasicDBObject("date", day);
			query.append("brand_id", brandId);
			if (new String("page").equalsIgnoreCase(publicDataJsonNode.get("objectType").asText()))
				{
				DBObject document = BasicDBObjectBuilder.start()
				
					.add("brand_id",brandId)
					.add("date",day)
					.add("name",publicDataJsonNode.get("displayName").asText())
					.add("image_url",publicDataJsonNode.get("image").get("url").asText())
					.add("plusOne",publicDataJsonNode.get("plusOneCount").asText())
					.add("circledBy",publicDataJsonNode.get("circledByCount").asText())		
					.get();
				googleplusCollection.update(query,new BasicDBObject("$set", document), true, false);
				}
			else
			{
				DBObject document = BasicDBObjectBuilder.start()
				
					.add("brand_id",brandId)
					.add("date",day)
					.add("name",publicDataJsonNode.get("displayName").asText())
					.add("image_url",publicDataJsonNode.get("image").get("url").asText())
					.add("circledBy",publicDataJsonNode.get("circledByCount").asText())		
					.get();
				googleplusCollection.update(query,new BasicDBObject("$set", document), true, false);
			}
					
						
		} catch (JSONParamsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}


	public void fetchPostData(String mediaId, String gplusId, Duration duration) {
		// TODO Auto-generated method stub
		System.out.println("Starting Fetch Google PLus Activities Data for "+gplusId+" for last "+duration.toString());
		
		DateTimeFormatter daf = DateTimeFormat.forPattern("yyyy-MM-dd");
		
		DateTime currentTime = new DateTime();
		
		DateTime fetchUpto = null;
		
		if(duration.equals(Duration.ONE_DAY)){
			
			fetchUpto = currentTime.minusDays(1);
		
		} else if(duration.equals(Duration.ONE_WEEK)){
		
			fetchUpto = currentTime.minusDays(7);
		
		} else if(duration.equals(Duration.ONE_MONTH)){
			
			fetchUpto = currentTime.minusMonths(1);
		
		} else if(duration.equals(Duration.THREE_MONTHS)){
			
			fetchUpto = currentTime.minusMonths(3);
		
		} else if(duration.equals(Duration.SIX_MONTHS)){
			
			fetchUpto = currentTime.minusMonths(6);
		
		} else if(duration.equals(Duration.ONE_YEAR)){
			
			fetchUpto = currentTime.minusYears(1);
		
		} else if(duration.equals(Duration.THREE_YEARS)){
			
			fetchUpto = currentTime.minusYears(3);
		
		} else if(duration.equals(Duration.FIVE_YEARS)){
			
			fetchUpto = currentTime.minusYears(5);
		}
		else if(duration.equals(Duration.THREE_DAYS)){
			
			fetchUpto = currentTime.minusDays(3);
		}
		
		/*
		 * Create and instantiate HTTP call objects
		 */
		HttpURLConnection connection = null;
		String activityData = null;
		String baseUrl = "https://www.googleapis.com/plus/v1/people/"
						+ gplusId
						+ "/activities/public?key=AIzaSyDCyoS7wsxZdSRCmUozarU435it8zJ5oaI";
		
		String jobUrl = baseUrl;
		
		
		try {
			
			boolean stopFetch = false;
			
			DateTime firstActivityTime = new DateTime();
			
			  while(
	    		!(stopFetch) && 
	    		(
	    			DateTime.parse(firstActivityTime.toString(daf))
	    			.isAfter(
	    					DateTime.parse(fetchUpto.toString(daf))
	    			)
	    		)
			  ){
				    
				  	URL url = new URL(jobUrl);
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty(StringConstants.CONTENT_TYPE, StringConstants.APPLICATION_JSON);
					InputStream responseStream = connection.getInputStream();
					
				    String charset = "UTF-8";
				    /*
				     * Fetching response data for the HTTP request
				     */
				    activityData = IOUtils.toString(responseStream, charset);
				    
			    JsonNode activityJsonNode = JSONOperations.castFromStringToJSONNode(activityData);
				    
				    
				    JsonNode activityDataJsonNode = activityJsonNode.get("items");
				    if(activityDataJsonNode.size() == 0){
				    System.out.println("\n Google Plus Activity Last Page Reached for "+gplusId);
				   	stopFetch = true;
				    } else {
				    	
				    	firstActivityTime = DateTime.parse(
							    			activityDataJsonNode
						    				.get(0)
						    				.get("published").asText()
				    					);
				    	
				    //	if(DateTime.parse(firstActivityTime.toString(daf)).isAfter(DateTime.parse(fetchUpto.toString(daf))))
				    	storeActivityData(activityDataJsonNode, gplusId);
				    	
				    	if(activityJsonNode.has("nextPageToken")){
				    		jobUrl = baseUrl+"&nextPageToken="+activityJsonNode.get("nextPageToken").asText();
				    		
				    	} else {
				    		stopFetch = true;
				    	}
				    }
				   
			  }
			  System.out.println("\nFetching Google Plus activity data completed for brand: "+gplusId);
		} catch (Exception e) {
			/*
			 * Exception caught while hitting the URL
			 * Error executing job.
			 */
			if(logger.isErrorEnabled())
				logger.error(StringConstants.ERROR_PERFORMING_URL_CALL + jobUrl, e);
			
			try {
				throw new JobExecutionFailedException(StringConstants.JOB_EXECUTION_FAILED + jobUrl, e);
			} catch (JobExecutionFailedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		if (logger.isDebugEnabled())
			logger.debug(StringConstants.EXIT_METHOD);
		
	
	}
public void storeActivityData(JsonNode activityDataJsonNode, String gplusId){
		
		BulkRequestBuilder bulkRequest = ElasticsearchService.transportClient.prepareBulk();
		
		for(int i=0; i < activityDataJsonNode.size() ; i++){
			
			JsonNode activityJsonNode = activityDataJsonNode.get(i);
			
			DateTime created_time = DateTime.parse(activityJsonNode.get("published").asText());
		
				
				Map<String, Object> source= new HashMap<String,Object>();
				
				source.put("brand_id", gplusId);
				source.put("published_time",created_time);
				System.out.println(source.get("published_time").toString());
				if(activityJsonNode.has("updated"))
					source.put("updated_time",activityJsonNode.get("updated").asText());
				if(activityJsonNode.has("title"))
				source.put("title",activityJsonNode.get("title").asText());
				if(activityJsonNode.has("url"))
				source.put("link",activityJsonNode.get("url").asText());
				if(activityJsonNode.has("id"))
				source.put("id",activityJsonNode.get("id").asText());
				if(activityJsonNode.has("kind"))
				source.put("kind",activityJsonNode.get("kind").asText());
				if(activityJsonNode.has("verb"))
					source.put("verb",activityJsonNode.get("verb").asText());
				if(activityJsonNode.has("annotation"))
					source.put("annotation",activityJsonNode.get("annotation").asText());
				if(activityJsonNode.get("object").has("content"))
					source.put("content",activityJsonNode.get("object").get("content").asText());
				if(activityJsonNode.get("object").has("objectType"))
					source.put("type",activityJsonNode.get("object").get("objectType").asText());
				if(activityJsonNode.get("object").has("url"))
					source.put("object_link",activityJsonNode.get("object").get("url").asText());
				if(activityJsonNode.get("object").has("replies"))
					source.put("replies",activityJsonNode.get("object").get("replies").get("totalItems").asText());
				if(activityJsonNode.get("object").has("plusoners"))
					source.put("plusOne",activityJsonNode.get("object").get("plusoners").get("totalItems").asText());
				if(activityJsonNode.get("object").has("resharers"))
					source.put("resharers",activityJsonNode.get("object").get("resharers").get("totalItems").asText());
				if(activityJsonNode.get("object").has("attachments"))
					{
					System.out.println(activityJsonNode.get("object").get("attachments"));
					System.out.println(activityJsonNode.get("object").get("attachments").size());
					for(int j=0;j<activityJsonNode.get("object").get("attachments").size();j++)
						{
						if(activityJsonNode.get("object").get("attachments").get(j).has("objectType"))
								{source.put("attachment_type",activityJsonNode.get("object").get("attachments").get(j).get("objectType").asText());
								
								}
						if(activityJsonNode.get("object").get("attachments").get(j).has("displayName"))
							source.put("attachment_name",activityJsonNode.get("object").get("attachments").get(j).get("displayName").asText());
						if(activityJsonNode.get("object").get("attachments").get(j).has("content"))
							source.put("attachment_content",activityJsonNode.get("object").get("attachments").get(j).get("content").asText());
						if(activityJsonNode.get("object").get("attachments").get(j).has("url"))
							source.put("attachment_url",activityJsonNode.get("object").get("attachments").get(j).get("url").asText());
						if(activityJsonNode.get("object").get("attachments").get(j).has("image"))
							source.put("picture",activityJsonNode.get("object").get("attachments").get(j).get("image").get("url").asText());
						}
					}
				
				int year = created_time.getYear();
				 
				String indexName = "googleplus-"+year;
				String mediaId = "gplusActivity"+source.get("id");
				
				bulkRequest.add(
						ElasticsearchService.transportClient
						.prepareIndex(indexName, "media", mediaId)
					    .setSource(source)
				);
			
		}
		
		if(bulkRequest.request().requests().size() == 0){
			
			System.out.println("\n\n No request Added!");
		
		} else{
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if (bulkResponse.hasFailures()) {
			    System.out.println("ElasticSearch Failures: \n"+bulkResponse.buildFailureMessage());
			}
			
		}
	}
	
	
}
