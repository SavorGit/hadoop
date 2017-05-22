/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 17:44
 */
package com.littlehotspot.hadoop.mr.nginx.mobile;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月19日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class NginxLog extends Configured implements Tool {
    //    private static Pattern BOX_LOG_FORMAT_REGEX = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) - - \\[(.+)\\] ([A-Z]+) ([^ ]+) HTTP/[^ ]+ \"(\\d{3})\" \\d+ \"(.+)\" \"(.+)\" \"(.+)\"$");
    private static Pattern BOX_LOG_FORMAT_REGEX = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) - [^ ]+ \\[(.+)\\] ([A-Z]+) ([^ ]+) HTTP/[^ ]+ \"(\\d{3})\" \\d+ \"(.+)\" \"([^-]+.*)\" \"(.+)\" \"(.+)\"$");

    private static class BoxMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static Pattern PARAMETER_FORMAT_REGEX = Pattern.compile("^(.+)=(.*)$");
        private static final String DATA_FORMAT_SRC = "dd/MMM/yyyy:HH:mm:ss Z";
        private static final String DATA_FORMAT_TAR = "yyyy-MM-dd HH:mm:ss Z";
        private static final char VALUE_SPLIT_CHAR = 1;

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            try {
                String msg = value.toString();
                Matcher matcher = BOX_LOG_FORMAT_REGEX.matcher(msg);
                if (!matcher.find()) {
                    return;
                }
                if (StringUtils.isBlank(matcher.group(7)) || "-".equalsIgnoreCase(matcher.group(7).trim())) {
                    return;
                }

                StringBuffer newValueStringBuffer = new StringBuffer();
                newValueStringBuffer.append(this.turnDataForNone(matcher.group(1))).append(VALUE_SPLIT_CHAR);// Client-IP
                newValueStringBuffer.append(this.toTimestamp(matcher.group(2))).append(VALUE_SPLIT_CHAR);// Access-Timestamp
                newValueStringBuffer.append(this.turnDataForNone(matcher.group(3))).append(VALUE_SPLIT_CHAR);// HTTP-Request-Method
                newValueStringBuffer.append(this.turnDataForNone(matcher.group(4))).append(VALUE_SPLIT_CHAR);// URI
                newValueStringBuffer.append(this.turnDataForNone(matcher.group(5))).append(VALUE_SPLIT_CHAR);// HTTP-Response-Status
                newValueStringBuffer.append(this.turnDataForNone(matcher.group(6))).append(VALUE_SPLIT_CHAR);// HTTP-Header[referer]
                newValueStringBuffer.append(this.analysisTraceInfo(matcher.group(7))).append(VALUE_SPLIT_CHAR);// HTTP-Header[traceinfo]
                newValueStringBuffer.append(this.turnDataForNone(matcher.group(8))).append(VALUE_SPLIT_CHAR);// HTTP-Header[user_agent]
                newValueStringBuffer.append(this.turnDataForNone(matcher.group(9))).append(VALUE_SPLIT_CHAR);// HTTP-Header[x_forwarded_for]
                newValueStringBuffer.append(this.turnDateFormat(matcher.group(2)));// Access-Time
                context.write(new Text(newValueStringBuffer.toString()), new Text());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private String analysisTraceInfo(String traceInfo) {
            StringBuffer traceInfoStringBuffer = new StringBuffer();
            if (StringUtils.isBlank(traceInfo)) {
                return traceInfoStringBuffer.toString();
            }
//            if (traceInfo.indexOf(';') < 0) {
//                return traceInfoStringBuffer.toString();
//            }
            Map<String, String> parameterMap = new ConcurrentHashMap<>();
            String[] parameterArray = traceInfo.split(";");
            for (String parameter : parameterArray) {
                if (StringUtils.isBlank(parameter)) {
                    continue;
                }
                Matcher matcher = PARAMETER_FORMAT_REGEX.matcher(parameter);
                if (!matcher.find()) {
                    continue;
                }
                parameterMap.put(matcher.group(1), matcher.group(2));
            }

            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "versionname", "")).append(VALUE_SPLIT_CHAR);// 版本名称
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "versioncode", "")).append(VALUE_SPLIT_CHAR);// 版本号
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "buildversion", "")).append(VALUE_SPLIT_CHAR);// 手机系统版本
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "osversion", "")).append(VALUE_SPLIT_CHAR);// 系统 API 版本
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "model", "")).append(VALUE_SPLIT_CHAR);// 机器型号
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "appname", "")).append(VALUE_SPLIT_CHAR);// 应用名称
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "clientname", "")).append(VALUE_SPLIT_CHAR);// 设备类型
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "channelid", "")).append(VALUE_SPLIT_CHAR);// 渠道 ID
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "channelName", "")).append(VALUE_SPLIT_CHAR);// 渠道名称
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "deviceid", "")).append(VALUE_SPLIT_CHAR);// 设备 ID
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "network", "")).append(VALUE_SPLIT_CHAR);// 网络类型
            traceInfoStringBuffer.append(this.getCustomParameterValue(parameterMap, "language", "")).append(VALUE_SPLIT_CHAR);// 语言
            return traceInfoStringBuffer.toString();
        }

        // NGINX 日志空数据转换
        private String turnDataForNone(String data) {
            if (StringUtils.isBlank(data)) {
                return "";
            }
            if ("-".equalsIgnoreCase(data)) {
                return "";
            }
            return data;
        }

        // NGINX 日志日期转时间截
        private long toTimestamp(String srcDateString) throws ParseException {
            String dateString = this.turnDataForNone(srcDateString);
            if (StringUtils.isBlank(dateString)) {
                return 0;
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATA_FORMAT_SRC, Locale.US);
            Date date = simpleDateFormat.parse(dateString);
            return date.getTime();
        }

        // NGINX 日志时间格式转换
        private String turnDateFormat(String srcDateString) throws ParseException {
            String dateString = this.turnDataForNone(srcDateString);
            if (StringUtils.isBlank(dateString)) {
                return "";
            }
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATA_FORMAT_SRC, Locale.US);
            Date date = simpleDateFormat.parse(dateString);
            String tarDateString = DateFormatUtils.format(date, DATA_FORMAT_TAR);
            return tarDateString;
        }

        // 获取 NGINX 日志自定义参数值
        private String getCustomParameterValue(Map<String, String> map, String key, String defaultValue) {
            if (map == null) {
                throw new IllegalArgumentException("The map is null");
            }
            String value = map.get(key);
            return value == null ? defaultValue : value;
        }
    }

    private static class BoxReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            try {
                context.write(key, new Text());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] arg) throws Exception {
        try {
            // 配置数据格式
            if (arg.length > 2) {
                BOX_LOG_FORMAT_REGEX = Pattern.compile(arg[2]);
            }

            Job job = Job.getInstance(this.getConf(), NginxLog.class.getSimpleName());
            job.setJarByClass(NginxLog.class);

            /**作业输入*/
            Path inputPath = new Path(arg[0]);
            FileInputFormat.addInputPath(job, inputPath);
            job.setMapperClass(BoxMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            Path outputPath = new Path(arg[1]);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), new Configuration());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(BoxReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
