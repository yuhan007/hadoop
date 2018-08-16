package com.vzoom.mapreduce.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TQMapper extends Mapper<LongWritable, Text, Weather, Text> {
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] str = value.toString().split("\t");
		try {
			Date date = sdf.parse(str[0]);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			int year = calendar.get(Calendar.YEAR);
			int month = calendar.get(Calendar.MONTH) + 1;
			int day = calendar.get(Calendar.DAY_OF_MONTH);
			String wdStr=str[1].substring(0, str[1].indexOf('c'));
			double wd = Double.parseDouble(wdStr);
			Weather weather = new Weather();
			weather.setYear(year);
			weather.setMonth(month);
			weather.setWd(wd);
			weather.setDay(day);

			context.write(weather, value);
		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

}
