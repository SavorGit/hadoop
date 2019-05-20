/**
 * Copyright (c) 2019, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.mapper
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:28
 */
package com.littlehotspot.hadoop.mr.hdfs.mapper;

import com.littlehotspot.hadoop.mr.hdfs.util.ConvertByRegexConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>Mapper - 利用正则表达式转换 HDFS 文件内容</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2019年05月17日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ConvertByRegexMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static SimpleDateFormat DATE_FORMAT_DATETIME = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);

    private Pattern PATTERN_DATETIME = Pattern.compile("\\d{2}/[A-Za-z]{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} \\+\\d{4}");
    private Pattern PATTERN_USER_AGENT_NET_TYPE = Pattern.compile("\\sNetType/([a-zA-Z\\d]*)\\s");
    private Pattern PATTERN_REQUEST_URI = Pattern.compile(" (/[\\S]+)\\?([\\S]+=[\\S]+) ");

    private Pattern PATTERN_OS_VERSION_IPHONE = Pattern.compile(" CPU iPhone OS ([0-9._-]+)");
    private Pattern PATTERN_OS_VERSION_ANDROID = Pattern.compile(" Android ([0-9._-]+)");
    private Pattern PATTERN_OS_VERSION_MAC = Pattern.compile(" Intel Mac OS X ([0-9._-]+)");
    private Pattern PATTERN_OS_VERSION_WINDOWS = Pattern.compile("Windows NT ([0-9._-]+)");

    private Pattern mapperInputFormatRegexPattern;

    /**
     * HDFS 文件内容转换 Mapper
     *
     * @param key     输入键
     * @param value   输入值
     * @param context Mapper 上下文
     * @throws IOException          输入输出异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.contains("|")) {
            return;
        }
        Matcher matcher = this.mapperInputFormatRegexPattern.matcher(line);
        if (!matcher.find()) {
            return;
        }
        StringBuilder valueStringBuilder = new StringBuilder();
        for (int number = 1; number <= matcher.groupCount(); number++) {
            String groupString = matcher.group(number);
            Matcher datetimeMatcher = PATTERN_DATETIME.matcher(groupString);
            if (datetimeMatcher.find()) {
                long timestamp = 0;
                try {
                    timestamp = DATE_FORMAT_DATETIME.parse(groupString).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                valueStringBuilder.append(timestamp).append("|");
            } else {
                valueStringBuilder.append(groupString).append("|");
            }
        }

        Matcher requestURIMatcher = PATTERN_REQUEST_URI.matcher(line);
        Map<String, String> parameterMap = new HashMap<>();
        if (requestURIMatcher.find()) {
//            String requestURI = requestURIMatcher.group(1);
            String queryString = requestURIMatcher.group(2);
            String[] parameters = queryString.split("&");
            for (String parameter : parameters) {
                String parameterName = null, parameterValue = null;
                if (parameter.contains("=")) {
                    parameterName = parameter.substring(0, parameter.indexOf("="));
                    parameterValue = parameter.substring(parameter.indexOf("=") + 1);
                } else {
                    parameterName = parameter;
                }
                if (StringUtils.isBlank(parameterName)) {
                    continue;
                }
                parameterMap.put(parameterName, parameterValue);
            }
        }
        if (parameterMap.containsKey("channel")) {
            valueStringBuilder.append(parameterMap.get("channel"));
        }
        valueStringBuilder.append("|");
        if (parameterMap.containsKey("issq")) {
            valueStringBuilder.append(parameterMap.get("issq"));
        }
        valueStringBuilder.append("|");

        // 是否微信
        if (line.contains(" MicroMessenger/")) {
            valueStringBuilder.append("1");
        }
        valueStringBuilder.append("|");

        // 获取连网方式
        Matcher netTypeMatcher = PATTERN_USER_AGENT_NET_TYPE.matcher(line);
        if (netTypeMatcher.find()) {
            valueStringBuilder.append(netTypeMatcher.group(1));
        }
        valueStringBuilder.append("|");

        // 获取操作系统
        Pattern osVersionPattern = null;
        if (line.contains(" CPU iPhone OS ")) {
            osVersionPattern = PATTERN_OS_VERSION_IPHONE;
            valueStringBuilder.append("ios");
        } else if (line.contains(" Android ")) {
            osVersionPattern = PATTERN_OS_VERSION_ANDROID;
            valueStringBuilder.append("android");
        } else if (line.contains(" Intel Mac OS X ")) {
            osVersionPattern = PATTERN_OS_VERSION_MAC;
            valueStringBuilder.append("mac");
        } else if (line.contains("Windows NT")) {
            osVersionPattern = PATTERN_OS_VERSION_WINDOWS;
            valueStringBuilder.append("windows");
        }
        valueStringBuilder.append("|");

        // 获取操作系统版本号
        if (osVersionPattern != null) {
            Matcher osVersionMatcher = osVersionPattern.matcher(line);
            if (osVersionMatcher.find()) {
                valueStringBuilder.append(osVersionMatcher.group(1));
            }
        }

//        context.write(value, NullWritable.get());
        context.write(new Text(valueStringBuilder.toString()), NullWritable.get());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //从全局配置获取配置参数
        Configuration conf = context.getConfiguration();
        this.mapperInputFormatRegexPattern = conf.getPattern(ConvertByRegexConstant.HadoopConfig.Key.MAPPER_INPUT_FORMAT_REGEX_PATTERN, null);
    }
}
