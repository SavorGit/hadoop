package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-27 下午 3:04.
 */
public class UserSadMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();

            Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX_FINAL.matcher(rowLineContent);
            if (!matcher.find()) {
                return;
            }

            String uuidMediaId = matcher.group(1);
            if (StringUtils.isBlank(uuidMediaId)) {
                return;
            }

            String act = matcher.group(6);

            StringBuffer stringBuffer = new StringBuffer(rowLineContent);
//            if(act.equals("start")){
//                stringBuffer.append()
//            }else{
//
//            }

            Text keyText = new Text(uuidMediaId);
//            System.out.println(rowLineContent);
            context.write(keyText, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
