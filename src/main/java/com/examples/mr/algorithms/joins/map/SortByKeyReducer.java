package com.examples.mr.algorithms.joins.map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Author: Stanley Wang
 * Date: 1/19/14
 * Time: 10:33 PM
 */
public class SortByKeyReducer extends Reducer<Text,Text,NullWritable,Text> {

    private static final NullWritable nullKey = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
             context.write(nullKey,value);
        }
    }
}
