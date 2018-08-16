package com.vzoom.mapreduce.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FofJobTwo {
	public static void main(String[] args) throws Exception {
		FofJobTwo driver = new FofJobTwo();
		driver.run();
	}

	public void run() throws Exception {
		// 该对象会默认读取环境中的 hadoop 配置。当然，也可以通过 set 重新进行配置
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:8020");
		conf.set("yarn.resourcemanager.hostname", "node3");

		// job 是 yarn 中任务的抽象。
		Job job = Job.getInstance(conf);

		// 指定本程序的jar包所在的本地路径
		job.setJarByClass(FofJobTwo.class);

		// 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FofMapperTwo.class);
		job.setReducerClass(FofReducerTwo.class);

		// 指定mapper输出数据的kv类型。需要和 Mapper 中泛型的类型保持一致
		job.setMapOutputKeyClass(Friend.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setSortComparatorClass(FofSort.class);
		job.setGroupingComparatorClass(FofGroup.class);
		// 指定job的输入原始文件所在目录
		FileInputFormat.addInputPath(job, new Path("/wc/output/"));
		// 指定job的输出结果所在目录
		Path output = new Path("/wc/output1");
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
