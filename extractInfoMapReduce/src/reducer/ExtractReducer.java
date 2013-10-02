package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.Tweet;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

public class ExtractReducer extends Reducer<IntWritable,Text,Text,NullWritable> {
	
	JsonParser parser = new JsonParser();
	Gson gson = new Gson();
	
	public void reduce(IntWritable key,Iterable <Text> values, Context context){
		ArrayList <Tweet> tList = new ArrayList <Tweet>();
		
		for(Text tweetTxt : values){
			Tweet tweet = gson.fromJson(tweetTxt.toString(), Tweet.class);
			tList.add(tweet);
		}
		
		Collections.sort(tList, new Comparator<Tweet>(){

			@Override
			public int compare(Tweet o1, Tweet o2) {
				// TODO Auto-generated method stub
				return o1.getDate().compareTo(o2.getDate());
			}
			
		});
		
		//String output = "Count:"+tList.size()+"  "+ tList.toString();
		
		try {
			for(Tweet tweet : tList){
				String output = tweet.getUserId()+"\t"+tweet.getDate().getTimeInMillis()
							    +"\t"+tweet.getLatitude()+"\t"+tweet.getLongitude()+
							    "\t"+tweet.getText()+"\t"+tweet.getUserName();
				context.write(new Text(output),NullWritable.get());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
