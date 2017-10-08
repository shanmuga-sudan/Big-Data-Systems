package org.hadoop.graphbuilder;

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
 
  public static void main(String[] args)  throws Exception
{ 
    JobClient client = new JobClient(); 
    JobConf conf = new JobConf(SpeciesGraphBuilder.class); 
    conf.setJobName("Page-rank Species Graph Builder"); 
 
    //conf.setOutputKeyClass(Text.class); 
    //conf.setOutputValueClass(Text.class); 
    conf.setMapperClass(SpeciesGraphBuilderMapper.class); 
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);
 
    //conf.setInputFormat(org.apache.hadoop.mapred.TextInputFormat.class); 
    //conf.setOutputFormat(org.apache.hadoop.mapred.SequenceFileOutputFormat.class); 

    conf.setReducerClass(SpeciesGraphBuilderReducer.class); 
    //conf.setCombinerClass(SpeciesGraphBuilderReducer.class); 
 
    //conf.setInputPath(new Path("graph1")); 
    //conf.setOutputPath(new Path("graph2")); 
    // take the input and output from the command line
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 
 
    client.setConf(conf); 
    try { 
      JobClient.runJob(conf); 
    } catch (Exception e) { 
      e.printStackTrace(); 
    } 
  } 
}  