package org.hadoop.pagerankmanipulation;

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
import java.io.IOException; 
import java.util.Iterator; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter; 
import java.lang.StringBuilder; 
import java.util.*; 

public class SpeciesGraphBuilderReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{ 

public void reduce(Text key, Iterator<Text> values, 
                   OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 

  reporter.setStatus(key.toString()); 
  String toWrite = ""; 
  int count = 0;

//  while (values.hasNext()) 
//  { 
//     String page = ((Text)values.next()).toString(); 
//     page.replaceAll(" ", "_"); 
//     toWrite += " " + page; 
//     count += 1; 
//  } 

  while (values.hasNext())
  {
     String page = ((Text)values.next()).toString(); 
     count = GetNumOutlinks(page);      
     page.replaceAll(" ", "_"); 
     toWrite += " " + page;
  } 

  IntWritable i = new IntWritable(count);
  String num = (i).toString(); 
  toWrite = num + ":" + toWrite; 
  output.collect(key, new Text(toWrite)); 
} 

 public int GetNumOutlinks(String page)
 {
     if (page.length() == 0)
         return 0;

     int num = 0;
     String line = page;
     int start = line.indexOf(" ");
     while (-1 < start && start < line.length())
     {
         num = num + 1;
         line = line.substring(start+1);
         start = line.indexOf(" ");
     }
     return num;
 }
} 

