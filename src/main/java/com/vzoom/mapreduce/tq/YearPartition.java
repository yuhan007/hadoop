package com.vzoom.mapreduce.tq;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class YearPartition extends HashPartitioner<Weather, Text>{

	@Override
	public int getPartition(Weather key, Text value, int numReduceTasks) {
		
		return (key.getYear()-1949)%numReduceTasks;
	}

}
