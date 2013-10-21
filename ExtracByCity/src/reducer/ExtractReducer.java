package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.gson.Gson;
import com.google.gson.JsonParser;

public class ExtractReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
	
	JsonParser parser = new JsonParser();
	Gson gson = new Gson();
	
	public void reduce(NullWritable key,Iterable <Text> values, Context context){
		for(Text value : values){
			try {
				context.write(value, NullWritable.get());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		
	}
}
