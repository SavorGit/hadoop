package com.littlehotspot.hadoop.mr.user.tag.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-09 上午 11:02.
 */
public class UserTagMapper extends Mapper<LongWritable, Text, Text, Text> {

    Pattern MAPPER_NGINX_LOG_FORMAT_REGEX = Pattern.compile("^(.*)\\|(\\d*)\\|([A-Za-z]*)\\|(.*)\\|(\\d*)\\|(.*)\\|([A-Za-z0-9.-_]*)\\|(\\d*)\\|([A-Za-z0-9.-_]*)\\|([A-Za-z0-9.-_]*)\\|(.*)\\|([A-Za-z0-9.-_]*)\\|([A-Za-z0-9.-]*)\\|([A-Za-z0-9.-_]*)\\|(.*)\\|([A-Za-z0-9.-_]*)\\|(\\d*)\\|([A-Za-z0-9.-_]*)\\|(.*)\\|(.*)\\|(.*)s*$");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rowLineContent = value.toString();
        Matcher matcher = MAPPER_NGINX_LOG_FORMAT_REGEX.matcher(rowLineContent);
        if (!matcher.find()) {
            return;
        }
        String deviceId = matcher.group(16);
        if (StringUtils.isBlank(deviceId)||deviceId.equals(null)||deviceId.equals("null")) {
            return;
        }

        String timestamp = matcher.group(2);

        context.write(new Text(deviceId), new Text(timestamp));
    }
}
