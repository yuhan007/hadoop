package com.vzoom.mapreduce.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 与 Mapper 类似，继承的同事声名四个泛型。 KEYIN, VALUEIN 对应 mapper输出的KEYOUT,VALUEOUT类型对应
 * KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型。此处 keyOut 表示单个单词，valueOut 对应的是总次数
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	/**
	 * <angelababy,1><angelababy,1><angelababy,1><angelababy,1><angelababy,1>
	 * <hello,1><hello,1><hello,1><hello,1><hello,1><hello,1> <banana,1>
	 * <banana,1><banana,1><banana,1><banana,1><banana,1> 入参key，是一组相同单词kv对的key
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
		}

		context.write(key, new IntWritable(count)); // 输出每一个单词出现的次数
	}

}
