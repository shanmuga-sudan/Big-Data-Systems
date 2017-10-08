package org.hadoop.xmlDataset;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

public class SpeciesGraphBuilderReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		reporter.setStatus(key.toString());
		String toWrite = "";
		//int count = 0;
		Double initPgRk=0.0;
		
		while (values.hasNext()) {
			String page = ((Text) values.next()).toString();
			page=page.replaceAll("=","");
			page=page.replaceAll("\\[","");
			page=page.replaceAll("\\]","");
			page=page.replaceAll("'","");
			page=page.replaceAll("\\|","");
			page=page.replaceAll(";","");
			page=page.replaceAll("\\?", "");
			page=page.replaceAll("\\/", "");
			page=page.replaceAll("\\(", "");
			page=page.replaceAll("\\)", "");
			page=page.replaceAll("\\#", "");
			page=page.replaceAll("\\!", "");
			page=page.replaceAll("\\,", "");
			page=page.replaceAll(".&nbsp", "");
			page=page.replaceAll("\\*.png", "");
			page=page.replaceAll("\\_", "");
			toWrite += " " + page;
			initPgRk+=0.4;
		}
			

		String num = initPgRk.toString();
		toWrite = num + ":" + toWrite;
		StringBuilder sb= new StringBuilder();
		
		  toWrite=toWrite.replaceAll("=","");
		  toWrite=toWrite.replaceAll("\\[","");
		  toWrite=toWrite.replaceAll("\\]","");
		  toWrite=toWrite.replaceAll("'","");
		  toWrite=toWrite.replaceAll("\\|","");
		  toWrite=toWrite.replaceAll(";","");
		  toWrite=toWrite.replaceAll("\\?", "");
		  toWrite=toWrite.replaceAll("\\/", "");
		  toWrite=toWrite.replaceAll("\\(", "");
		  toWrite=toWrite.replaceAll("\\)", "");
		  toWrite=toWrite.replaceAll("\\#", "");
		  toWrite=toWrite.replaceAll("\\!", "");
		  toWrite=toWrite.replaceAll("\\,", "");
		  toWrite=toWrite.replaceAll(".&nbsp", "");
		  toWrite=toWrite.replaceAll("\\*.png", "");
		  toWrite=toWrite.replaceAll("\\_", "");
		  String[] myLine=toWrite.split(" ");
			String[] unique = new HashSet<String>(Arrays.asList(myLine)).toArray(new String[0]);
			for(int iter=0;iter<unique.length;iter++){
				if(!(unique[iter].length()<3))
					sb.append(" ");
				sb.append(unique[iter]);
			}
		  if(!key.toString().isEmpty()) // ||(temp_ValueKey.toString().length()>10
		output.collect(new Text(key), new Text(toWrite.trim()));
	}
	
}
