package geocoding;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GeoDriver{
	
	private static final String HDFSPATH = "/user/hive/warehouse/checkintest/";
	private static final String HDFSOUT = "/user/hive/warehouse/checkintest/output/";
	
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();

		conf.set("hadoop.job.ugi", "hadoop,supergroup");
		conf.set("mapred.job.tracker", "bigdata-node01.cs.fiu.edu:30001");
		conf.set("mapred.map.tasks", "100");

		Job job = new Job(conf, "geocoding");
		job.setJarByClass(GeoDriver.class);
		job.setMapperClass(GeoMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(HDFSPATH));
		
		FileSystem fs = FileSystem.get(job.getConfiguration());
		
		Path oPath = new Path(HDFSOUT);
		
		if(fs.exists(oPath)){
			fs.delete(oPath);
		}
		
		FileOutputFormat.setOutputPath(job, oPath);
		
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}
