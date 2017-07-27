package com.littlehotspot.hadoop.mr.box.hbase.mapper;

import com.littlehotspot.hadoop.mr.box.common.CommonVariables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * 数据到hbasemapper
 */
public class BoxToHbaseMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();
            Matcher matcher = CommonVariables.MAPPER_LOG_INTEGRATED_FORMAT_REGEX.matcher(rowLineContent);
            if (!matcher.find()) {
                return;
            }
            String boxMac = matcher.group(13); //机顶盒mac
            String timestamp=matcher.group(4); //时间戳
            Text keyText = new Text(boxMac+timestamp);
            context.write(keyText, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
