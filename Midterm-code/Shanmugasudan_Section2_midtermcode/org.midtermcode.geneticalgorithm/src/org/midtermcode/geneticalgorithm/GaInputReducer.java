package org.midtermcode.geneticalgorithm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.midtermcode.geneticalgorithm.Chomosone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class GaInputReducer extends Reducer<DoubleWritable, Chomosone, Text, IntWritable> {
	@Override
	protected void reduce(DoubleWritable fitnessScore, Iterable<Chomosone> values,Context context)
						throws IOException, InterruptedException 
	{
		Configuration conf=context.getConfiguration();
		String targetValue=conf.get("target");
		ArrayList<Chomosone> myArrayList= new ArrayList<Chomosone>();
		
		int gen=0;
		
		int size = 0;
		for(Chomosone value : values) {
		   size++;
//		   System.out.println(value.getChromo().length()+" is the value at reducer length");
		}
		
		Iterator itr= values.iterator();
		while(itr.hasNext()){
			
			myArrayList.add((Chomosone) itr.next());
		}
		ArrayList newPool = new ArrayList(myArrayList.size());
		boolean flag=false;
//		while(!flag)
//		{
		newPool.clear();

		for(int x=myArrayList.size()-1;x>=0;x-=2) {
			gen++;
	
			
//			System.out.println("THe size of my arraylist :"+myArrayList.size());
			
				
			
			Chomosone n1 = Chomosone.selectMember(myArrayList);
			Chomosone n2 = Chomosone.selectMember(myArrayList);
//			System.out.println(n2.getDecodeChromo());
			
//			if(!(n1==null||n2==null)){
			
			// Check to see if either is the solution
//			System.out.println("The value of n1 and n2 : "+n1.getTotal()+" "+n2.getTotal());
			if (n1.getTotal() == Integer.parseInt(targetValue) && n1.isValid()) { 
				System.out.println("Generations: " + gen + "  Solution: " + n1.decodeChromo()); 
				context.write(new Text(n1.getDecodeChromo().toString()), new IntWritable(gen));
				flag=true;
				return; 
				}
		    if (n2.getTotal() == Integer.parseInt(targetValue) && n2.isValid()) { 
				System.out.println("Generations: " + gen + "  Solution: " + n2.decodeChromo());
				context.write(new Text(n1.getDecodeChromo().toString()), new IntWritable(gen));
				flag=true;
				 return;
				}
			
				// Cross over and mutate
				n1.crossOver(n2);
				n1.mutate();
				n2.mutate();
				
				// Rescore the nodes
				System.out.println("Checked out of score");
				n1.scoreChromo(Integer.parseInt(targetValue));
				n2.scoreChromo(Integer.parseInt(targetValue));
				
				
				// Add to the new pool
				newPool.add(n1);
				newPool.add(n2);
			
//			}
		}
			myArrayList.addAll(newPool);
//			
//		
//			
//		
//			}
			
		
		
	}
	
	
	

}
