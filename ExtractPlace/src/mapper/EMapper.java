package mapper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EMapper extends Mapper<Object,Text,Text,NullWritable>{
	
	private String removeOther(String place){
		int t2 = 0;
		if((t2 = place.indexOf("w/"))!=-1){
			place = place.substring(0, t2);
		}
		if((t2=place.indexOf("- @"))!=-1){
			place = place.substring(0, t2);
		}
		if((t2=place.indexOf("for"))!=-1){
			place = place.substring(0,t2);
		}
		if((t2=place.indexOf("#"))!=-1){
			place = place.substring(0,t2);
		}
		
		return place;
		
	}
	
private String extract(String line){
		String placeName = null;
		//boolean flag = false;
		if(line.contains("I'm at")){
		//	flag = true;
			int l = 0;
			if((l=line.indexOf('('))!=-1){
				String place = line.substring(6, l).trim();
				place = removeOther(place);
				placeName = place;
			//	System.out.println("aaaaaa "+place);
			}
		}
		else if(line.contains("(@")||line.contains("(at")){
			//flag = true;
			
			int s = line.indexOf("(@");
			int t1 = line.indexOf(')', s);
			
			String place = line.substring(s+2, t1);
			place = removeOther(place);
			placeName = place;
			//System.out.println("@aaaaaa "+place);
			
		}
		
		
		return placeName;
	}
	
	public void map(Object object,Text line,Context context){
		String [] columns = line.toString().split("\t");
		String text = null;
		int len = columns.length;
		if(columns.length>=5){
			text = columns[4];
			String country = columns[len-3];
			String placeName = "none";
			if(country.equals("US") || country.equals("United States")){
				placeName = extract(text);
			}
			//System.out.println(text);
			try {
				context.write(new Text(line.toString().trim()+'\t'+placeName), NullWritable.get());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	  }
	
}
