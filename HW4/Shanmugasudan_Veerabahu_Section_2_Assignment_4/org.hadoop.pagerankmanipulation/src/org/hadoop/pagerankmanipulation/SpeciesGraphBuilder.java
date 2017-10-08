package org.hadoop.pagerankmanipulation;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

//

// Author - Jack Hebert (jhebert@cs.washington.edu) 
// Copyright 2007 
// Distributed under GPLv3 
// 
//Modified - Dino Konstantopoulos
//Distributed under the "If it works, remolded by Dino Konstantopoulos, 
//otherwise no idea who did! And by the way, you're free to do whatever 
//you want to with it" dinolicense
//

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.File;

import org.apache.hadoop.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

//import org.apache.nutch.parse.Parse; 
//import org.apache.nutch.parse.ParseException; 
//import org.apache.nutch.parse.ParseUtil; 
//import org.apache.nutch.protocol.Content; 

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class SpeciesGraphBuilder {

	public static void main(String[] args) throws Exception {	
		
		//Job 1
		JobClient client = new JobClient();
		JobConf conf = new JobConf(SpeciesGraphBuilder.class);
		
		conf.setJobName("Page-rank Species Graph Builder");
		conf.setMapperClass(SpeciesGraphBuilderMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setReducerClass(SpeciesGraphBuilderReducer.class);
		
		Path input = new Path(args[0]);
		Path inter_Output_1 = new Path(args[1]);
		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, inter_Output_1);
		client.setConf(conf);
		
		//Job 2
		
		
		JobConf conf2 = new JobConf(SpeciesGraphBuilder.class);
		FileSystem fs = FileSystem.get(conf2);
		conf2.setJobName("Species Iter"); 

		conf2.setNumReduceTasks(5); 

		conf2.setOutputKeyClass(Text.class); 
		conf2.setOutputValueClass(Text.class); 
		
		
		conf2.setMapperClass(SpeciesIterMapper2.class); 
		conf2.setReducerClass(SpeciesIterReducer2.class); 
		conf2.setCombinerClass(SpeciesIterReducer2.class); 

		Path inter_Output_2 = new Path(args[2]);
		client.setConf(conf2);
		
		//Job 3
		  JobConf conf3 = new JobConf(SpeciesGraphBuilder.class); 
		  conf3.setJobName("Species Viewer"); 

		  //~dk
		  //conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class); 

		  conf3.setOutputKeyClass(FloatWritable.class); 
		  conf3.setOutputValueClass(Text.class); 

		  Path inter_Output_3 = new Path(args[3]);
		  FileInputFormat.setInputPaths(conf3, inter_Output_2);
		  FileOutputFormat.setOutputPath(conf3, inter_Output_3);

		  conf3.setMapperClass(SpeciesViewerMapper.class); 
		  conf3.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class); 

		  client.setConf(conf3); 

		try {
			JobClient.runJob(conf);
			
//			FsPermission fileSystemPermission= new FsPermission(FsAction.ALL,FsAction.ALL,FsAction.ALL);
			
			for(int iterationCounter=0;iterationCounter<=1;iterationCounter++){
				
				
				if(iterationCounter!=0){
					
//					FileSystem fileSystem=inter_Output_1.getFileSystem(conf2);
//					Path fileSystemPath=inter_Output_1.getParent();
					if(fs.exists(inter_Output_2)) 
					{
						
//						FSDataOutputStream temp_File=FileSystem.create(fileSystem,fileSystemPath,fileSystemPermission);
//						fs.delete(inter_Output_1, true);
						fs.delete(inter_Output_1,true);
						fs.copyToLocalFile(inter_Output_2, inter_Output_1);
						fs.delete(inter_Output_2, true);
						System.err.println("Output file deleted");
					}
				fs.close();
					FileInputFormat.setInputPaths(conf2, inter_Output_1);
					FileOutputFormat.setOutputPath(conf2, inter_Output_2);
					JobClient.runJob(conf2);
				}
				else{
					
					FileInputFormat.setInputPaths(conf2, inter_Output_1);
					FileOutputFormat.setOutputPath(conf2, inter_Output_2);
					JobClient.runJob(conf2);
				}
			}
			
				
			JobClient.runJob(conf3);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
