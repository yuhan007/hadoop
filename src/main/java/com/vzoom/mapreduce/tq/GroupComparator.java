package com.vzoom.mapreduce.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
	GroupComparator() {
		super(Weather.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Weather weather1 = (Weather) a;
		Weather weather2 = (Weather) b;
		int result = Integer.compare(weather1.getYear(), weather2.getYear());
		if (result == 0) {
			return Integer.compare(weather1.getMonth(), weather2.getMonth());
		} else {
			return result;
		}
	}

}
