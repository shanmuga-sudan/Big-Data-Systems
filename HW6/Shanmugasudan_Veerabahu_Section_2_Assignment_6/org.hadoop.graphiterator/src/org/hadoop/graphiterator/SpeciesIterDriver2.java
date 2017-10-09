package org.hadoop.graphiterator;

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
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapred.JobClient; 
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.Reducer; 

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


public class SpeciesIterDriver2 { 

public static void main(String[] args) { 
JobClient client = new JobClient(); 
JobConf conf = new JobConf(SpeciesIterDriver2.class); 
conf.setJobName("Species Iter"); 

conf.setNumReduceTasks(5); 

//~dk
//conf.setInputFormat(org.apache.hadoop.mapred.SequenceFileInputFormat.class); 
//conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class); 

conf.setOutputKeyClass(Text.class); 
conf.setOutputValueClass(Text.class); 

if (args.length < 2) { 
System.out.println("Usage: PageRankIter <input path> <output path>"); 
System.exit(0); 
} 

//~dk
//conf.setInputPath(new Path(args[0])); 
//conf.setOutputPath(new Path(args[1])); 


//conf.setInputPath(new Path("graph2")); 
//conf.setOutputPath(new Path("graph3")); 

conf.setMapperClass(SpeciesIterMapper2.class); 
conf.setReducerClass(SpeciesIterReducer2.class); 
conf.setCombinerClass(SpeciesIterReducer2.class); 

client.setConf(conf); 
try { 
	for(int iter=0;iter<10;iter++){
		if(iter!=0){
			FileInputFormat.setInputPaths(conf, "output"+iter);
			FileOutputFormat.setOutputPath(conf, new Path("output"+(iter+1)));
			JobClient.runJob(conf); 
		}
		else{
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
			JobClient.runJob(conf); 
		}
	}
} catch (Exception e) { 
e.printStackTrace(); 
} 
} 
} 
