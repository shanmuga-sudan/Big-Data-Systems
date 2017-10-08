package org.midtermcode.geneticalgorithm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class GaDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		
	
		
		Configuration conf = new Configuration();
		conf.set("target", args[2]);
//		System.out.println("the target value is : "+args[2]);
		Job job = new Job(conf);
			
		
		job.setJarByClass(GaDriver.class);
		
		// Need to set this since, map out is different from reduce out
		job.setMapOutputKeyClass(DoubleWritable.class); 
		job.setMapOutputValueClass(Chomosone.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
		job.setMapperClass(GaInputMapper.class);
		job.setReducerClass(GaInputReducer.class);
		job.setPartitionerClass(GaPartitioner.class);
				
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
				
		FileInputFormat.addInputPath(job, input);
		
		FileOutputFormat.setOutputPath(job, output);
				
		job.waitForCompletion(true);
				
		System.out.println("MR Job Completed !");
		
	}
}
