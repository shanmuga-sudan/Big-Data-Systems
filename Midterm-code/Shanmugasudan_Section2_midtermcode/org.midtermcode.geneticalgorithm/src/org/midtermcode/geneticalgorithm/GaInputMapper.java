package org.midtermcode.geneticalgorithm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import org.midtermcode.geneticalgorithm.Chomosone;

import com.sun.org.apache.xerces.internal.impl.xpath.regex.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class GaInputMapper extends Mapper<LongWritable,Text,DoubleWritable,Chomosone> {
	
	// Static info
	
	
	static int poolSize = 400;	// Must be even
	@Override
	protected void map(LongWritable key, Text value,Context context)
						throws IOException, InterruptedException
	{
		Configuration conf=context.getConfiguration();
		String targetValue=conf.get("target");
	for (int x=0;x<poolSize;x++) {
		Chomosone ct=new Chomosone();
		ct.getMyChomosone(Integer.parseInt(targetValue));
		double valueRounded=0.0;
		double fitnessScore=ct.getScore();
		if(!(fitnessScore>1)){
			valueRounded= Math.round(ct.getScore() * 100D) / 100D;
		}
		else{
			valueRounded=Math.round(valueRounded);
//			System.out.println(valueRounded+"is rounded");
		}
		if(valueRounded>=0.3)
		context.write(new DoubleWritable(valueRounded),ct);
//		System.out.println(valueRounded+"is the key"+ct+" is the value");
		
	}
	
	}

	
}
