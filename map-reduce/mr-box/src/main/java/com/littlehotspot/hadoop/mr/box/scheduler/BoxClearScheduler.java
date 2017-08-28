/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:31
 */
package com.littlehotspot.hadoop.mr.box.scheduler;

import com.littlehotspot.hadoop.mr.box.common.Argument;
import com.littlehotspot.hadoop.mr.box.reducer.BoxClearReducer;
import com.littlehotspot.hadoop.mr.box.util.Constant;
import com.littlehotspot.hadoop.mr.box.mapper.BoxClearMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;

/**
 * 调度器 - 机顶盒日志
 */
public class BoxClearScheduler extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            Constant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
            // 获取参数
            String hdfsInputPath = Constant.CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = Constant.CommonVariables.getParameterValue(Argument.OutputPath);
            this.getConf().setLong("mapreduce.input.fileinputformat.split.maxsize",128*1024*1024);
            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());


            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.setInputPaths(job, inputPath);
            job.setMapperClass(BoxClearMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()),this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setInputFormatClass(CombineTextInputFormat.class);
            job.setReducerClass(BoxClearReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

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

}
