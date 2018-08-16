package com.vzoom.mapreduce.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortComparator extends WritableComparator {

	public SortComparator() {
		super(Weather.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		Weather ia = (Weather) a;
		Weather ib = (Weather) b;

		int result = Integer.compare(ia.getYear(), ib.getYear());
		if (result == 0) {
			result = Integer.compare(ia.getMonth(), ib.getMonth());
			if (result == 0) {
				return -Double.compare(ia.getWd(), ib.getWd());
			} else {
				return result;
			}
		} else
			return result;
	}

}
