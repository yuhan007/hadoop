package com.vzoom.mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FofMapperOne extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] strs = value.toString().split(" ");
		Fof fof=new Fof();
		for (int i = 1; i < strs.length; i++) {
			//一度的好友关系
			String s1=fof.format(strs[0], strs[i]);
			context.write(new Text(s1), new IntWritable(0));
			//二度关系
			for (int j = i+1; j < strs.length; j++) {
				String s2=fof.format(strs[i], strs[j]);
				context.write(new Text(s2), new IntWritable(1));
			}
		}
		
		
	}
}
