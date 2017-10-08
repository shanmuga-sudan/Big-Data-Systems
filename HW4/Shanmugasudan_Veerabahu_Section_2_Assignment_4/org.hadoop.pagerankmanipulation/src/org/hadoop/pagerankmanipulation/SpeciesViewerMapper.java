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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter; 
import org.apache.hadoop.io.*; 

public class SpeciesViewerMapper extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text> { 

public void map(WritableComparable key, Writable value, 
                OutputCollector output, Reporter reporter) throws IOException { 

  // get the current page
  String data = ((Text)value).toString(); 
  int index = data.indexOf(":"); 
  if (index == -1) { 
    return; 
  } 

  // split into title and PR (tab or variable number of blank spaces)
  String pagetitle="";
  String toParse = data.substring(0, index).trim(); 
  String[] splits = toParse.split("\t"); 
  if(splits.length == 0) {
    splits = toParse.split(" ");
         if(splits.length == 0) {
            return;
         }
  }
//  Pattern pattern=Pattern.compile("^[0-9]+([a-zA-Z]+)^[0-9]+^[=).*]");   //
  String pagetitle_temp = splits[0].trim();
  pagetitle=pagetitle_temp.replaceAll("[^A-Za-z]","").trim();
//  Matcher matcher=pattern.matcher(pagetitle_temp);
//  if(matcher.find()){
//   pagetitle=matcher.group(2);
//  }
//  else{
//	   pagetitle=pagetitle_temp;
//  }
  String pagerank = splits[splits.length - 1].trim();

  // parse score
  double currScore = 0.0;
  try { 
     currScore = Double.parseDouble(pagerank); 
  } catch (Exception e) { 
     currScore = 0.0;
  } 

  // collect
 // output.collect(new FloatWritable((float) - currScore), key); 
  if(!((pagetitle.isEmpty())&&(pagetitle!="")))
  output.collect(new FloatWritable((float) - currScore), new Text(pagetitle)); 
} 
} 
