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
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.*;
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
public class UserFinal extends Configured implements Tool {

    private static class MobileMapper extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String msg = value.toString();
                Matcher userMatcher = CommonVariables.MAPPER_USER_FORMAT_REGEX.matcher(msg);
                Matcher actMatcher = CommonVariables.MAPPER_ACT_FORMAT_REGEX.matcher(msg);
                if (userMatcher.find()) {
                    context.write(new Text(userMatcher.group(1)), value);
                }else if (actMatcher.find()){
                    context.write(new Text(actMatcher.group(1)), value);

                }else {
                    return;
                }


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
                TargetUserBean targetUserBean = new TargetUserBean();
                TargetUserAttrBean targetUserAttrBean = new TargetUserAttrBean();
                TargetUserActiBean targetUserActiBean = new TargetUserActiBean();
                while (iterator.hasNext()){
                    Text item = iterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher userMatcher = CommonVariables.MAPPER_USER_FORMAT_REGEX.matcher(rowLineContent);
                    Matcher actMatcher = CommonVariables.MAPPER_ACT_FORMAT_REGEX.matcher(rowLineContent);
                    if (userMatcher.find()){
                        targetUserBean.setRowKey(userMatcher.group(1));
                        targetUserAttrBean.setDeviceId(userMatcher.group(1));
                        targetUserAttrBean.setDeviceType(userMatcher.group(2));
                        targetUserAttrBean.setMachineModel(userMatcher.group(3));
                        targetUserAttrBean.setToken(userMatcher.group(6));
                        targetUserActiBean.setDownloadTime(userMatcher.group(4));
                        targetUserActiBean.setSince(userMatcher.group(5));
                    }else if (actMatcher.find()){
                        if (!StringUtils.isBlank(actMatcher.group(4))&&actMatcher.group(4).equals("pro")){
                            targetUserActiBean.setProjectionTime(actMatcher.group(2));
                            targetUserActiBean.setProjectionCount(actMatcher.group(3));
                        }else if (!StringUtils.isBlank(actMatcher.group(4))&&actMatcher.group(4).equals("dema")){
                            targetUserActiBean.setDemandTime(actMatcher.group(2));
                            targetUserActiBean.setDemandCount(actMatcher.group(3));
                        }else if (!StringUtils.isBlank(actMatcher.group(4))&&actMatcher.group(4).equals("read")){
                            targetUserActiBean.setReadTime(actMatcher.group(2));
                            targetUserActiBean.setReadCount(actMatcher.group(3));
                        }
                    }

                }

                targetUserBean.setTargetUserAttrBean(targetUserAttrBean);
                targetUserBean.setTargetUserActiBean(targetUserActiBean);
                CommonVariables.hBaseHelper.insert(targetUserBean);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
            CommonVariables.hBaseHelper = new HBaseHelper(this.getConf());

            // 获取参数
            String matcherRegex = CommonVariables.getParameterValue(Argument.MapperInputFormatRegex);
            String hdfsInputPath1 = CommonVariables.getParameterValue(Argument.UserInputPath);
            String hdfsInputPath2 = CommonVariables.getParameterValue(Argument.ProInputPath);
            String hdfsInputPath3 = CommonVariables.getParameterValue(Argument.DemaInputPath);
            String hdfsInputPath4 = CommonVariables.getParameterValue(Argument.ReadInputPath);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), UserFinal.class.getSimpleName());
            job.setJarByClass(UserFinal.class);

            /**作业输入*/
            Path inputPath1 = new Path(hdfsInputPath1);
            Path inputPath2 = new Path(hdfsInputPath2);
            Path inputPath3 = new Path(hdfsInputPath3);
            Path inputPath4 = new Path(hdfsInputPath4);
            FileInputFormat.addInputPath(job, inputPath1);
            FileInputFormat.addInputPath(job, inputPath2);
            FileInputFormat.addInputPath(job, inputPath3);
            FileInputFormat.addInputPath(job, inputPath4);
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
