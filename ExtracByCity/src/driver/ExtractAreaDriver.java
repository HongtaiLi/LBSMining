package driver;

import mapper.ExtractMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import reducer.ExtractReducer;


public class ExtractAreaDriver{
	
	private static String inputPath = null;
	private static String outputPath = null;
	
	
	public static void main(String args[]) throws Exception{
		if(args.length!=3){
			System.out.println("Invalid Arguments:"+args.length);
			return;
		}
		System.out.println("InputPath:"+args[1]);
		System.out.println("OutputPath:"+args[2]);
		
		
		inputPath = args[1];
		outputPath = args[2];
		
		
		Configuration conf = new Configuration();

		conf.set("hadoop.job.ugi", "hadoop,supergroup");
		conf.set("mapred.job.tracker", "bigdata-node01.cs.fiu.edu:30001");
		conf.set("mapred.map.tasks", "50");

		Job job = new Job(conf, "ExtractByArea");
		job.setJarByClass(ExtractAreaDriver.class);
		job.setMapperClass(ExtractMapper.class);
		job.setReducerClass(ExtractReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		
		FileSystem fs = FileSystem.get(job.getConfiguration());
		
		Path oPath = new Path(outputPath);
		
		if(fs.exists(oPath)){
			fs.delete(oPath);
		}
		
		FileOutputFormat.setOutputPath(job, oPath);
		
		job.setNumReduceTasks(10);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		
		job.waitForCompletion(true);
	}
}
