package driver;

import java.io.IOException;

import mapper.ExtractMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import reducer.ExtractReducer;

public class ExtractDriver {
	
	private static final String HDFSPATH = "/user/hli019/4sq/";
	private static final String HDFSOUT = "/user/hli019/4sq/output/";
	
	
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();

		conf.set("hadoop.job.ugi", "hadoop,supergroup");
		conf.set("mapred.job.tracker", "bigdata-node01.cs.fiu.edu:30001");
		conf.set("mapred.map.tasks", "100");

		Job job = new Job(conf, "ExtractInfo");
		job.setJarByClass(ExtractDriver.class);
		job.setMapperClass(ExtractMapper.class);
		job.setReducerClass(ExtractReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(HDFSPATH));
		
		FileSystem fs = FileSystem.get(job.getConfiguration());
		
		Path oPath = new Path(HDFSOUT);
		
		if(fs.exists(oPath)){
			fs.delete(oPath);
		}
		
		FileOutputFormat.setOutputPath(job, oPath);

		job.setNumReduceTasks(50);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.waitForCompletion(true);
	}
}
