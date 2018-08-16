package com.vzoom.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordcountDriver {
	public static void main1() throws Exception {
		WordcountDriver driver=new WordcountDriver();
		driver.run();
	}

	public  void run() throws Exception {
		// 该对象会默认读取环境中的 hadoop 配置。当然，也可以通过 set 重新进行配置
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:8020");
		conf.set("yarn.resourcemanager.hostname", "node3");

		// job 是 yarn 中任务的抽象。
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		job.setJarByClass(WordcountDriver.class);
		job.setCombinerClass(WordcountReducer.class);
		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);

		// 指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 指定最终输出的数据的kv类型。这里也是 Reduce 的 key，value类型。
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 指定job的输入原始文件所在目录
		FileInputFormat.addInputPath(job, new Path("/wc/input/wc.txt"));
		// 指定job的输出结果所在目录
		Path output = new Path("/wc/output");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		FileOutputFormat.setOutputPath(job, output);
		boolean res = job.waitForCompletion(true);
		if (res) {
			System.out.println("job success!");
		}
	}
}
