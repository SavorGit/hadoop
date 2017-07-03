package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * <h1> 用户行为过滤Mapper </h1>
 * <p>
 * Created by Administrator on 2017-06-23 上午 11:28.
 */
public class SadActMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = value.toString();

            Configuration conf = context.getConfiguration();
            SadActType sadActType = SadActType.valueOf(conf.get("sadActType"));
            Matcher matcher;
            switch (sadActType) {
                case START_PRO:
                    matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX_START_PRO.matcher(rowLineContent);
                    break;
                case END_PRO:
                    matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX_END_PRO.matcher(rowLineContent);
                    break;
                case START_DEM:
                    matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX_START_DEM.matcher(rowLineContent);
                    break;
                case END_DEM:
                    matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX_END_DEM.matcher(rowLineContent);
                    break;
                default:
                    matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(rowLineContent);
                    break;
            }

            if (!matcher.find()) {
                return;
            }
            String uuid = matcher.group(1);
            if (StringUtils.isBlank(uuid)) {
                return;
            }
            String mediaId = matcher.group(7);
            if (StringUtils.isBlank(mediaId)) {
                return;
            }
            String timestamp = matcher.group(4);
            if (StringUtils.isBlank(timestamp)) {
                return;
            }

            Long time;
            if (sadActType.equals(SadActType.END_DEM) || SadActType.END_PRO.equals(sadActType)) {
                time = 9999999999999L - Long.decode(timestamp);
            } else {
                time = Long.decode(timestamp);
            }

            Text keyText = new Text(uuid + mediaId + time);

//            System.out.println(rowLineContent);
            context.write(keyText, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}