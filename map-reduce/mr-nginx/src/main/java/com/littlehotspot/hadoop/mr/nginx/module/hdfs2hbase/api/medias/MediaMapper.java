package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias;

import com.littlehotspot.hadoop.mr.nginx.util.Constant;
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

        String rowLineContent = value.toString();
        if (StringUtils.isBlank(rowLineContent.trim()) && "null".equals(rowLineContent)) {
            return;
        }

        String keyString = rowLineContent.substring(0,rowLineContent.indexOf(Constant.VALUE_SPLIT_CHAR));

        context.write(new Text(keyString), value);
    }
}
