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

import org.apache.hadoop.io.Writable; 
import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.mapred.MapReduceBase; 
import org.apache.hadoop.mapred.Mapper; 
import org.apache.hadoop.mapred.OutputCollector; 
import org.apache.hadoop.mapred.Reporter; 
import org.apache.hadoop.io.Text; 


public class SpeciesIterMapper2 extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text> { 

public void map(WritableComparable key, Writable value, 
                OutputCollector output, Reporter reporter) throws IOException { 

  // get the current page
  String data = ((Text)value).toString(); 
  int index = data.indexOf(":"); 
  if (index == -1) { 
    return; 
  } 

  // split into title and PR (tab or variable number of blank spaces)
  String toParse = data.substring(0, index).trim(); 
  String[] splits = toParse.split("\t"); 
  if(splits.length == 0) {
    splits = toParse.split(" ");
         if(splits.length == 0) {
            return;
         }
  }
  String pagetitle = splits[0].trim(); 
  String pagerank = splits[splits.length - 1].trim();
  
  // parse current score
  double currScore = 0.0;
  try { 
     currScore = Double.parseDouble(pagerank); 
  } catch (Exception e) { 
     currScore = 1.0;
  } 

  // get number of outlinks
  data = data.substring(index+1); 
  String[] pages = data.split(" "); 
  int numoutlinks = 0;
  if (pages.length == 0) {
     numoutlinks = 1;
  } else {
    for (String page : pages) { 
      if(page.length() > 0) {
         numoutlinks = numoutlinks + 1;
      }
    } 
  }

  // collect each outlink, with the dampened PR of its inlink, and its inlink
  Text toEmit = new Text((new Double(.01 * currScore / numoutlinks)).toString()); 
  for (String page : pages) { 
    if(page.length() > 0) {
      output.collect(new Text(page), toEmit); 
      output.collect(new Text(page), new  Text(" " + pagetitle)); 
    }
  } 

  // collect the inlink with its dampening factor, and all outlinks
  output.collect(new Text(pagetitle), new Text(".02")); 
  output.collect(new Text(pagetitle), new Text(" " + data)); 
} 
} 
