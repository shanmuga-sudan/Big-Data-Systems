package org.hadoop.pagerankmanipulation;
//
//Author - Jack Hebert (jhebert@cs.washington.edu) 
//Copyright 2007 
//Distributed under GPLv3 
//
//Modified - Dino Konstantopoulos
//Distributed under the "If it works, remolded by Dino Konstantopoulos, 
//otherwise no idea who did! And by the way, you're free to do whatever 
//you want to with it" dinolicense
//
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.*; 
import java.lang.StringBuilder; 

/* 
* This class reads in a serialized download of wikispecies, extracts out the links, and 
* foreach link: 
*   emits (currPage, (linkedPage, 1)) 
* 
* 
*/ 
public class SpeciesGraphBuilderMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> { 


public void map(LongWritable key, Text value, 
                OutputCollector output, Reporter reporter) throws IOException
{
  // Prepare the input data. 
  String page = value.toString(); 

  System.out.println("Page:" + page); 
  String title = this.GetTitle(page, reporter); 
  if (title.length() > 0) { 
    reporter.setStatus(title); 
  } else { 
    return; 
  } 

  ArrayList<String> outlinks = this.GetOutlinks(page); 
  StringBuilder builder = new StringBuilder(); 
  for (String link : outlinks) { 
    link = link.replace(" ", "_"); 
    builder.append(" "); 
    builder.append(link); 
  } 
  output.collect(new Text(title), new Text(builder.toString())); 
} 

public String GetTitle(String page, Reporter reporter) throws IOException{ 
         int end = page.indexOf(",");
         if (-1 == end)
             return "";
         return page.substring(0, end);
} 

public ArrayList<String> GetOutlinks(String page){ 
  int end; 
  ArrayList<String> outlinks = new ArrayList<String>(); 
  int start=page.indexOf("[["); 
  while (start > 0) { 
    start = start+2; 
    end = page.indexOf("]]", start); 
    //if((end==-1)||(end-start<0)) 
    if (end == -1) { 
      break; 
    } 

    String toAdd = page.substring(start); 
    toAdd = toAdd.substring(0, end-start); 
    outlinks.add(toAdd); 
    start = page.indexOf("[[", end+1); 
  } 
  return outlinks; 
} 
}


