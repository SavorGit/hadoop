package com.littlehotspot.hadoop.mr.box.hbase.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 数据到hbasemapper
 */
public class BoxToHbaseMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            context.write(value, new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
