package org.midtermcode.geneticalgorithm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class GaPartitioner extends
	Partitioner<DoubleWritable, Chomosone> {

@Override
public int getPartition(DoubleWritable key, Chomosone value,
		int numReduceTasks) {

	return (key.hashCode() % numReduceTasks);
}


}
