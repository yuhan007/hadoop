package com.vzoom.mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FofReducerTwo extends Reducer<Friend, IntWritable, Text, NullWritable>{

	@Override
	protected void reduce(Friend friend, Iterable<IntWritable> value,
			Context context) throws IOException, InterruptedException {
		for (IntWritable i : value) {
			String msg=friend.getFriend1()+"-"+friend.getFriend2()+":"+friend.getHot();
			context.write(new Text(msg), NullWritable.get());
		}
	}

}
