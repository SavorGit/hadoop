package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-10 下午 6:12.
 */
public class MediaMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {

            if(StringUtils.isBlank(value.toString().trim())) {
                return;
            }

            context.write(new Text(key.toString()), value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
