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

import java.io.IOException; 
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter;

import jdk.nashorn.internal.runtime.regexp.joni.Regex;

import java.lang.StringBuilder; 
import java.util.*; 

public class SpeciesGraphBuilderReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{ 

public void reduce(Text key, Iterator<Text> values, 
                   OutputCollector<Text, Text> output, Reporter reporter) throws IOException { 

  reporter.setStatus(key.toString()); 
  String toWrite = ""; 
  int count = 0;

  while (values.hasNext()) 
  { 
     String page = ((Text)values.next()).toString(); 
     page.replaceAll(" ", "_"); 
     page.replaceAll(":", "_");  //Added newly for testing 
     toWrite += " " + page; 
     count += 1; 
  } 

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
  if((key.toString().contains("Template:")))
  {
	  key = new Text(key.toString().replace("Template:", ""));
  }
  else if (key.toString().contains(":Templates"))
  {
	  key = new Text(key.toString().replace(":Templates", ""));
  }
  String charsToRemove = "%^#";
  String stringToFilter = "I have 20% of my assets in #2 pencils! :^)";

  //toWrite = CharMatcher.anyOf(charsToRemove).removeFrom(stringToFilter);
  
//  Pattern p = Pattern.compile("|");
//  String regexPattern=p.toString();
  
  //toWrite = toWrite.replaceAll("[;]+[=]+[|]","");
  //Regex re = new Regex("[;\\\\/:*?\"<>|&']");
  //toWrite = re.(toWrite, " ");
  toWrite = toWrite.replaceAll("=","");
  toWrite = toWrite.replaceAll("\\[","");
  toWrite = toWrite.replaceAll("\\]","");
  toWrite = toWrite.replaceAll("'","");
  toWrite = toWrite.replaceAll("\\|","");
  toWrite = toWrite.replaceAll(";","");
  
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
