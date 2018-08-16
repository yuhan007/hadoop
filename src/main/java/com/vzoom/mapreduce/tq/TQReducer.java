package com.vzoom.mapreduce.tq;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TQReducer extends Reducer<Weather, Text, NullWritable,Text > {

	@Override
	protected void reduce(Weather weather, Iterable<Text> value, Context context)
			throws IOException, InterruptedException {
		int i = 0;
		for (Text t : value) {
			i++;
			if (i > 2)
				break;
			context.write(NullWritable.get(),t );
		}
	}

}
