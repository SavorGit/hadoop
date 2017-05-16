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
package com.littlehotspot.hadoop.mr.box;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月16日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class BoxLog extends Configured implements Tool {

    private static Pattern BOX_LOG_FORMAT_REGEX = Pattern.compile("^.+,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.*,.+,.*,?$");

    private static class BoxMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            /**数据清洗=========开始*/
            try {
                String msg = value.toString();
                Matcher matcher = BOX_LOG_FORMAT_REGEX.matcher(msg);
                if (!matcher.find()) {
                    return;
                }
                context.write(value, new Text());
            } catch (Exception e) {
                // TODO: handle exception
                e.toString();
            }
        }
    }

    private static class BoxReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            try {
                context.write(key, new Text());
            } catch (Exception e) {
                // TODO: handle exception
                e.toString();
            }
        }
    }

    @Override
    public int run(String[] arg) throws Exception {
        // TODO Auto-generated method stub
        try {
            // 配置数据格式
            if (arg.length > 2) {
                BOX_LOG_FORMAT_REGEX = Pattern.compile(arg[2]);
            }

            Job job = Job.getInstance(this.getConf(), BoxLog.class.getSimpleName());
            job.setJarByClass(BoxLog.class);

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
            // TODO: handle exception
            e.toString();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();

        // 配置 HDFS 根路径
        if (args.length > 3) {
            conf.set("fs.defaultFS", args[3]);
//            conf.set("fs.defaultFS", "hdfs://devpd1:8020");
        }

        try {
            ToolRunner.run(conf, new BoxLog(), args);
        } catch (Exception e) {
            // TODO: handle exception
            e.toString();
        }
    }
}
