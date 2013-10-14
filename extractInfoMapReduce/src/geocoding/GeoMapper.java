package geocoding;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.httpclient.HttpClient;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GeoMapper extends Mapper<Object,Text,NullWritable,Text>{
	
	public void setup(Context context){
		
	}
	
	private String sendGet(String url) throws Exception {
		
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
 
		// optional default is GET
		con.setRequestMethod("GET");
		//add request header
		//con.setRequestProperty("User-Agent", USER_AGENT);
 
		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);
 
		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine = null;
		StringBuffer response = new StringBuffer();
		int cnt = 0;
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
			cnt++;
			if(cnt==1){
				break;
			}
		}
		in.close();
		
		//print result
		//System.out.println(response.toString());
		return inputLine;
	}

	
	public void map(Object key,Text line,Context context){
		String [] columns =  line.toString().split("\t");
		int len = columns.length;
		String country = columns[len-1];
		String lat;
		String lon;
		String codingQuery = "http://stree.cs.fiu.edu/street?x1=val1&y1=val2";
		
		if(country.equals("US") || country.equals("United States")){
			
			lat = columns[2];
			lon = columns[3];

			codingQuery.replaceAll("val1", lon);
			codingQuery.replaceAll("val2", lat);
			 try {
				String street = sendGet(codingQuery);
				String [] fields =  street.split("\t");
				String houseNo = fields[1];
				String sName = fields[4];
				context.write(NullWritable.get(), new Text(line.toString()+'\t'+sName+'\t'+houseNo));
			 } catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		}
				
		
	}

}
