package com.vzoom.mapreduce.tq2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 排序类，继承WritableComparator
 * 排序规则【年-月-温度】 温度降序
 * Created by Edward on 2016/7/11.
 */
public class SortComparator extends WritableComparator {

    /**
     * 调用父类的构造函数
     */
    SortComparator()
    {
        super(InfoWritable.class, true);
    }


    /**
     * 比较两个对象的大小，使用降序排列
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        InfoWritable ia = (InfoWritable)a;
        InfoWritable ib = (InfoWritable)b;

        int result = Integer.compare(ia.getYear(),ib.getYear());
        if(result == 0)
        {
            result = Integer.compare(ia.getMonth(),ib.getMonth());
            if(result == 0)
            {
                return -Double.compare(ia.getTemperature(), ib.getTemperature());
            }
            else
                return result;
        }
        else
            return result;
    }
}