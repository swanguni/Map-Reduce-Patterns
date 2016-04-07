package com.examples.mr.algorithms.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Author: Stanley Wang
 * Date: 1/7/13
 * Time: 11:00 PM
 */
public class YearMonthGroupingComparator extends WritableComparator {


    public YearMonthGroupingComparator() {
        super(TemperaturePair.class, true);
    }


    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
        TemperaturePair temperaturePair = (TemperaturePair) tp1;
        TemperaturePair temperaturePair2 = (TemperaturePair) tp2;
        return temperaturePair.getYearMonth().compareTo(temperaturePair2.getYearMonth());
    }
}
