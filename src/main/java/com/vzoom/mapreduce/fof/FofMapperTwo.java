package com.vzoom.mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FofMapperTwo extends Mapper<LongWritable, Text, Friend, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String []strs=value.toString().split("-");
		Friend f1=new Friend();
		
		f1.setFriend1(strs[0]);
		f1.setFriend2(strs[1]);
		f1.setHot(Integer.parseInt(strs[2]));
		context.write(f1,new IntWritable(f1.getHot()) );
		
		Friend f2=new Friend();
		f2.setFriend1(strs[1]);
		f2.setFriend2(strs[0]);
		f2.setHot(Integer.parseInt(strs[2]));
		context.write(f2,new IntWritable(f2.getHot()) );
	}

}
