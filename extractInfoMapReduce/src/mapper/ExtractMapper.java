package mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonParseException;

import util.Tweet;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ExtractMapper extends Mapper<Object,Text,IntWritable,Text>{
	
	public void setup(Context context){
		
	}
	
	Gson gson= new Gson();
	JsonParser parser = new JsonParser();
	
	
	public void map(Object key,Text line,Context context){
		JsonObject tweetObj = null;
		try{
			tweetObj =  parser.parse(line.toString().trim()).getAsJsonObject();
		}catch(Exception e){
			e.printStackTrace();
			return;
		}
		
		Tweet tweet;
		try {
			if(tweetObj!=null){
				tweet = extractInfo(tweetObj);
				if(tweet.getUserId()!=-1){
					context.write(new IntWritable(tweet.getUserId()),new Text(gson.toJson(tweet)));
				}
			}
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private Tweet extractInfo(JsonObject tweetObj) throws ParseException{
		
		  int userId = -1;
		  double lat = 0.0;
		  double lon = 0.0;
		  String text = null;
		  String userName = null;
		  Calendar date = null;
		  
		  if(tweetObj.has("user")&&tweetObj.get("user")!=null){
			  
			 JsonObject userObj =  tweetObj.get("user").getAsJsonObject();
			 
			 if(userObj.has("id")&&userObj.get("id")!=null){
				 userId = userObj.get("id").getAsInt();
			 }
			 
			 if(userObj.has("name")&&userObj.get("name")!=null){
				 userName = userObj.get("name").getAsString();
			 }
		  }
		  
		  System.out.println("ttttt"+tweetObj);
		  System.out.println("xxxxx"+tweetObj.get("coordinates"));
		  
		  if(tweetObj.has("coordinates")&&tweetObj.get("coordinates")!=null){
			  JsonObject coorObj = tweetObj.get("coordinates").getAsJsonObject();
			  if(coorObj.has("coordinates")&&coorObj.get("coordinates")!=null){
				  JsonArray locArray = coorObj.get("coordinates").getAsJsonArray();
				  lon = locArray.get(0).getAsDouble();
				  lat = locArray.get(1).getAsDouble();
			  }
		  }
		  
		  if(tweetObj.has("text")&&tweetObj.get("text")!=null){
			  text = tweetObj.get("text").getAsString();
		  }
		  
		  if(tweetObj.has("created_at")&&tweetObj.get("created_at")!=null){
			  String createAt = tweetObj.get("created_at").getAsString();
			  final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
			  SimpleDateFormat sf = new SimpleDateFormat(TWITTER);
			  sf.setLenient(true);
			  date = Calendar.getInstance(); 
			  date.setTime(sf.parse(createAt)); 
			  
		  }
		  
		  Tweet tweet = new Tweet(userId,userName,lat,lon,text,date);
		  return tweet;
		  
	}
	

}
