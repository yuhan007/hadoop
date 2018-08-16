package com.vzoom.mapreduce.tq2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * weather 统计天气信息
 *
 * 数据： 1999-10-01 14:21:02 34c 1999-11-02 13:01:02 30c
 *
 * 要求： 将每年的每月中气温排名前三的数据找出来
 *
 * 实现： 1.每一年用一个reduce任务处理; 2.创建自定义数据类型，存储 [年-月-日-温度]; 2.自己实现排序函数 根据 [年-月-温度]
 * 降序排列，也可以在定义数据类型中进行排序; 3.自己实现分组函数，对 [年-月]
 * 分组，reduce的key是分组结果，根据相同的分组值，统计reduce的value值，只统计三个值就可以，因为已经实现了自排序函数。
 *
 * Created by Edward on 2016/7/11.
 */
public class JobRun {

	public static void main1() {
		JobRun jobRun=new JobRun();
		jobRun.run();
	}

	public void run() {
		// access hdfs's user
		System.setProperty("HADOOP_USER_NAME", "root");

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:8020");
		conf.set("yarn.resourcemanager.hostname", "node3");

		try {
			FileSystem fs = FileSystem.get(conf);

			Job job = Job.getInstance(conf);
			job.setJarByClass(JobRun.class);
			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);

			// 需要指定 map out 的 key 和 value
			job.setOutputKeyClass(InfoWritable.class);
			job.setOutputValueClass(Text.class);

			// 设置分区 继承 HashPartitioner
			job.setPartitionerClass(YearPartition.class);
			// 根据年份创建指定数量的reduce task
			job.setNumReduceTasks(1);

			// 设置排序 继承 WritableComparator
			// job.setSortComparatorClass(SortComparator.class);

			// 设置分组 继承 WritableComparator 对reduce中的key进行分组
			job.setGroupingComparatorClass(GroupComparator.class);

			FileInputFormat.addInputPath(job, new Path("/wc/input/tq.txt"));

			Path path = new Path("/wc/output");
			if (fs.exists(path)) // 如果目录存在，则删除目录
			{
				fs.delete(path, true);
			}
			FileOutputFormat.setOutputPath(job, path);

			boolean b = job.waitForCompletion(true);
			if (b) {
				System.out.println("OK");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class MyMapper extends Mapper<LongWritable, Text, InfoWritable, Text> {

		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");

			try {
				Date date = sdf.parse(str[0]);
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				int year = c.get(Calendar.YEAR);
				int month = c.get(Calendar.MONTH) + 1;
				int day = c.get(Calendar.DAY_OF_MONTH);

				double temperature = Double.parseDouble(str[1].substring(0, str[1].length() - 1));

				InfoWritable info = new InfoWritable();
				info.setYear(year);
				info.setMonth(month);
				info.setDay(day);
				info.setTemperature(temperature);

				context.write(info, value);

			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	public static class MyReducer extends Reducer<InfoWritable, Text, Text, NullWritable> {
		@Override
		protected void reduce(InfoWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int i = 0;
			for (Text t : values) {
				i++;
				if (i > 3)
					break;
				context.write(t, NullWritable.get());
			}
		}
	}
}