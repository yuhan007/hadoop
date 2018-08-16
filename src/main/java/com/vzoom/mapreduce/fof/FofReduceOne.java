package com.vzoom.mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FofReduceOne extends Reducer<Text, IntWritable, Text, NullWritable> {

	@Override
	protected void reduce(Text text, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		
		int sum=0;//亲密度
		boolean flag=true;
		for (IntWritable i : value) {
			if (i.get()==0) {
				flag=false;
				break;
			}
			sum++;
		}
		if (flag) {
			String msg=text.toString()+"-"+sum;
			context.write(new Text(msg), NullWritable.get());
		}
		
	}

}
