/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:38
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.CommonVariables;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.NgxSrcUserBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.SrcUserBean;
import org.apache.commons.lang.StringUtils;
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

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class AllUser extends Configured implements Tool {

    private static class MobileMapper extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String msg = value.toString();
                Matcher matcher = CommonVariables.MAPPER_USER_FORMAT_REGEX.matcher(msg);
                if (!matcher.find()) {
                    return;
                }

                if (StringUtils.isBlank(matcher.group(1))) {
                    return;
                }

                context.write(new Text(matcher.group(1)), value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class Combiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Text> iterator = value.iterator();
                SrcUserBean srcUserBean = new SrcUserBean();
                while (iterator.hasNext()){
                    Text item = iterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    srcUserBean.setValue(rowLineContent);
                }
                context.write(new Text(srcUserBean.getDeviceId()), new Text(srcUserBean.rowLine()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Text> iterator = value.iterator();
                SrcUserBean srcUserBean = new SrcUserBean();
                while (iterator.hasNext()){
                    Text item = iterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher matcher = CommonVariables.MAPPER_USER_FORMAT_REGEX.matcher(rowLineContent);
                    if (!matcher.find()) {
                        return;
                    }
                    srcUserBean.setDeviceId(matcher.group(1));
                    if (!StringUtils.isBlank(matcher.group(2))){
                        srcUserBean.setMType(matcher.group(2));
                    }

                    if (!StringUtils.isBlank(matcher.group(3))){
                        srcUserBean.setMMachine(matcher.group(3));
                    }

                    if (StringUtils.isBlank(srcUserBean.getFDownTime())||(!StringUtils.isBlank(matcher.group(4))&&Long.valueOf(srcUserBean.getFDownTime())>Long.valueOf(matcher.group(4)))){
                        srcUserBean.setFDownTime(matcher.group(4));
                        srcUserBean.setFDownSrc("ngx");
                    }
                    srcUserBean.setValue(rowLineContent);
                }
                context.write(new Text(srcUserBean.rowLine()), new Text());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String matcherRegex = CommonVariables.getParameterValue(Argument.MapperInputFormatRegex);
            String hdfsInputPath1 = CommonVariables.getParameterValue(Argument.InputBoxPath);
            String hdfsInputPath2 = CommonVariables.getParameterValue(Argument.InputMobPath);
            String hdfsInputPath3 = CommonVariables.getParameterValue(Argument.InputNgxPath);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), AllUser.class.getSimpleName());
            job.setJarByClass(AllUser.class);

            /**作业输入*/
            Path inputPath1 = new Path(hdfsInputPath1);
            Path inputPath2 = new Path(hdfsInputPath2);
            Path inputPath3 = new Path(hdfsInputPath3);
            FileInputFormat.addInputPath(job, inputPath1);
            FileInputFormat.addInputPath(job, inputPath2);
            FileInputFormat.addInputPath(job, inputPath3);
            job.setMapperClass(MobileMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(MobileReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    public static boolean isYesterday(long time) {
        boolean isYesterday = false;
        Date date;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            date = sdf.parse(sdf.format(new Date()));
            if (time < date.getTime() && time > (date.getTime() - 24*60*60*1000)) {
                isYesterday = true;
            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return isYesterday;
    }
}
