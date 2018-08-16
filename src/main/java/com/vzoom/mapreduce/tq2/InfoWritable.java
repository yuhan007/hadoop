package com.vzoom.mapreduce.tq2;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义数据类型 继承 WritableComparable
 * 【年-月-日-温度】
 * Created by Edward on 2016/7/11.
 */
public class InfoWritable implements WritableComparable<InfoWritable> {

    private int year;
    private int month;
    private int day;
    private double temperature;

    public void setYear(int year) {
        this.year = year;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getDay() {
        return day;
    }

    public double getTemperature() {
        return temperature;
    }

    /**
     *
     * 对象比较，对温度进行倒序排序
     */
    @Override
    public int compareTo(InfoWritable o) {

        int result = Integer.compare(this.getYear(),o.getYear());
        if(result == 0)
        {
            result = Integer.compare(this.getMonth(),o.getMonth());
            if(result == 0)
            {
                return -Double.compare(this.getTemperature(), o.getTemperature());
            }
            else
                return result;
        }
        else
            return result;

        //return this==o?0:-1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.month);
        dataOutput.writeInt(this.day);
        dataOutput.writeDouble(this.temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.temperature = dataInput.readDouble();
    }
}