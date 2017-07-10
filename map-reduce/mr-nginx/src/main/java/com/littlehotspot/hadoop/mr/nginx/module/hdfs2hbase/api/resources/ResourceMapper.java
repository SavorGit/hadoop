package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-06 下午 4:02.
 */
public class ResourceMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();
//            Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(rowLineContent);
//            if (!matcher.find()) {
//                return;
//            }

            context.write(new Text(key.toString()), value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
