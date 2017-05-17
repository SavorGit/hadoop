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
package com.littlehotspot.hadoop.mr.mobile;

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
 * 手机日志
 */
public class MobileLog extends Configured implements Tool {

    private static class MobileMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Pattern MOBILE_LOG_FORMAT_REGEX = Pattern.compile("^.+,.*,.*,.*,.*,.*,.*,.*,.+,.*,.*,.*,.*,.*,.*,.*,?$");

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            /**数据清洗=========开始*/
            try {
                String msg = value.toString();
                Matcher matcher = MOBILE_LOG_FORMAT_REGEX.matcher(msg);
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

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {

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
            Job job = Job.getInstance(this.getConf(), MobileLog.class.getSimpleName());
            job.setJarByClass(MobileLog.class);

            /**作业输入*/
            Path inputPath = new Path(arg[0]);
            FileInputFormat.addInputPath(job, inputPath);
            job.setMapperClass(MobileMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            Path outputPath = new Path(arg[1]);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), new Configuration());
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
        } catch (Exception e) {
            // TODO: handle exception
//            e.toString();
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//		distributedCache
        try {
            ToolRunner.run(conf, new MobileLog(), args);
        } catch (Exception e) {
            // TODO: handle exception
            e.toString();
        }
    }
}
