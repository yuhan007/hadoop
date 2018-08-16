package com.vzoom.mapreduce.tq2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 创建分组类，继承WritableComparator
 * 【年-月】
 * Created by Edward on 2016/7/11.
 */
public class GroupComparator extends WritableComparator {

    GroupComparator()
    {
        super(InfoWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        InfoWritable ia = (InfoWritable)a;
        InfoWritable ib = (InfoWritable)b;

        int result = Integer.compare(ia.getYear(),ib.getYear());
        if(result == 0)
        {
            return Integer.compare(ia.getMonth(),ib.getMonth());
        }
        else
            return result;
    }
}