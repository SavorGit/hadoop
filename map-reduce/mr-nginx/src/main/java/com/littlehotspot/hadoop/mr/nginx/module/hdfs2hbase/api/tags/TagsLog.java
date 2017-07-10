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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.tags;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read.TargetUserReadAttrBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read.TargetUserReadRelaBean;
import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.*;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.CategoryService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.ContentService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.HotelService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.RoomService;
import org.apache.commons.lang.StringUtils;
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
import org.mortbay.util.ajax.JSON;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class TagsLog extends Configured implements Tool {


    private static class MobileMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String msg = value.toString();
                Matcher tagMatcher = CommonVariables.MAPPER_TAG_LOG_FORMAT_REGEX.matcher(msg);
                Matcher taglistMatcher = CommonVariables.MAPPER_TAGLIST_LOG_FORMAT_REGEX.matcher(msg);
                if (tagMatcher.find()) {
                    context.write(new Text(tagMatcher.group(1)), value);
                }else if (taglistMatcher.find()){
                    context.write(new Text(taglistMatcher.group(1)), value);
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

                Configuration conf = context.getConfiguration();
                Iterator<Text> textIterator = value.iterator();
                TargetTagBean targetTagBean = new TargetTagBean();
                TargetTagAttrBean targetTagAttrBean = new TargetTagAttrBean();
                List<TagSourceBean> list = new ArrayList<>();
                while (textIterator.hasNext()) {
                    TagSourceBean tagSourceBean = new TagSourceBean();
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher tagMatcher = CommonVariables.MAPPER_TAG_LOG_FORMAT_REGEX.matcher(rowLineContent);
                    Matcher taglistMatcher = CommonVariables.MAPPER_TAGLIST_LOG_FORMAT_REGEX.matcher(rowLineContent);
                    if (taglistMatcher.find()) {
                        targetTagBean.setRowKey(taglistMatcher.group(1));
                        targetTagAttrBean.setName(taglistMatcher.group(2));
                    }else if (tagMatcher.find()){
                        if (!StringUtils.isBlank(tagMatcher.group(1))){
                            tagSourceBean.setResource_id(tagMatcher.group(1));
                        }

                        if (!StringUtils.isBlank(tagMatcher.group(1))){
                            tagSourceBean.setResource_type("0x0001");
                        }



                    }
                    if (null!=tagSourceBean.getResource_id()||null!=tagSourceBean.getResource_type()){
                        list.add(tagSourceBean);
                    }

                }
                if (list.size()>0){
                    targetTagAttrBean.setResources(JSON.toString(list));
                }
                targetTagBean.setTargetTagAttrBean(targetTagAttrBean);
                CommonVariables.hBaseHelper.insert(targetTagBean);

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
            String hdfsInputStart = CommonVariables.getParameterValue(Argument.InputPathStart);
            String hdfsInputEnd = CommonVariables.getParameterValue(Argument.InputPathEnd);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), TagsLog.class.getSimpleName());
            job.setJarByClass(TagsLog.class);

            /**作业输入*/
            Path inputPath1 = new Path(hdfsInputStart);
            FileInputFormat.addInputPath(job, inputPath1);
            Path inputPath2 = new Path(hdfsInputEnd);
            FileInputFormat.addInputPath(job, inputPath2);
            job.setMapperClass(MobileMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
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
