package mapper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExtractMapper extends Mapper<Object,Text,Text,NullWritable>{
	//750	1348348304000	37.59717957	-122.503686	Food: meh. View: Amazing!!! (@ Taco Bell) [pic]: http://t.co/XCbnfkUG	cg	SHELTER COVE	CALIFORNIA	US

	private boolean filterByArea(String [] columns){
		int len = columns.length;
		
		if(len>3){
			String state = columns[len-2];
			String city = columns[len-3];
			String country = columns[len-1];
			if(country.toLowerCase().equals("us") || country.toLowerCase().equals("united states")){
				if((state.toLowerCase().equals("new york") || state.toLowerCase().equals("ny") )||
						(city.toLowerCase().equals("new york") || city.toLowerCase().equals("ny"))){
							return true;
				}
			}
			
			
			
		}
		return false;
	}
	public void map(Object object,Text line,Context context){
		String [] columns = line.toString().split("\t");
		String text = null;
		//int len = columns.length;
		if(filterByArea(columns)){
			try {
				context.write(line,NullWritable.get());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
	}
	
}
