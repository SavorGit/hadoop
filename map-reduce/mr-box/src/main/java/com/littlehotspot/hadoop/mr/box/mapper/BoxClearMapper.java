package com.littlehotspot.hadoop.mr.box.mapper;

import com.littlehotspot.hadoop.mr.box.common.CommonVariables;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * 清洗机顶盒日志文件mapper
 */
public class BoxClearMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    Text ckey=new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();
            if(rowLineContent.indexOf("null")>-1){
                rowLineContent=rowLineContent.replaceAll("null","");
            }
            Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(rowLineContent);
            if (!matcher.find()) {
                return;
            }
            ckey.set(rowLineContent);
            context.write(ckey, NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
