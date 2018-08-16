package com.vzoom.mapreduce.tq2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 *
 * 创建分区，通过key中的year来创建分区
 *
 * Created by Edward on 2016/7/11.
 */
public class YearPartition extends HashPartitioner <InfoWritable, Text>{
    @Override
    public int getPartition(InfoWritable key, Text value, int numReduceTasks) {
        return key.getYear()%numReduceTasks;
    }
}