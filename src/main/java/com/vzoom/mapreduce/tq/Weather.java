package com.vzoom.mapreduce.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Weather implements WritableComparable<Weather> {
	private int year;
	private int month;
	private int day;
	private double wd;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.year);
		out.writeInt(this.month);
		out.writeInt(this.day);
		out.writeDouble(this.wd);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readDouble();
	}

	/**
	 *
	 * 对象比较，对温度进行倒序排序
	 */
	@Override
	public int compareTo(Weather o) {
		
		int result = Integer.compare(this.getYear(), o.getYear());
		if (result == 0) {
			result = Integer.compare(this.getMonth(), o.getMonth());
			if (result == 0) {
				return -Double.compare(this.getWd(), o.getWd());
			} else
				return result;
		} else{
			return result;
		}
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public double getWd() {
		return wd;
	}

	public void setWd(double wd) {
		this.wd = wd;
	}


}
