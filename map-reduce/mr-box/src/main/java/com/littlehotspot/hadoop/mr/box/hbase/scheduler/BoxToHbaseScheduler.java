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
package com.littlehotspot.hadoop.mr.box.hbase.scheduler;

import com.littlehotspot.hadoop.mr.box.common.Argument;
import com.littlehotspot.hadoop.mr.box.hbase.mapper.BoxToHbaseMapper;
import com.littlehotspot.hadoop.mr.box.hbase.reducer.BoxToHbaseReducer;
import com.littlehotspot.hadoop.mr.box.util.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;

/**
 * 调度器 - 机顶盒日志
 */
public class BoxToHbaseScheduler extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        try {
            Constant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
            // 获取参数
            String hbaseSharePath = Constant.CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = Constant.CommonVariables.getParameterValue(Argument.HDFSCluster);

            String hdfsInputPath = Constant.CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = Constant.CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            // 避免报错：ClassNotFoundError hbaseConfiguration
            Configuration jobConf = job.getConfiguration();
            FileSystem hdfs = FileSystem.get(new URI(hdfsCluster), jobConf);
            Path hBaseSharePath = new Path(hbaseSharePath);
            FileStatus[] hBaseShareJars = hdfs.listStatus(hBaseSharePath);
            for (FileStatus fileStatus : hBaseShareJars) {
                if (!fileStatus.isFile()) {
                    continue;
                }
                Path archive = fileStatus.getPath();
                FileSystem fs = archive.getFileSystem(jobConf);
                DistributedCache.addArchiveToClassPath(archive, jobConf, fs);
            }


            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);
            job.setMapperClass(BoxToHbaseMapper.class);
//            job.setMapperClass(BoxToHbaseMapper2.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()),this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(BoxToHbaseReducer.class);
//            job.setReducerClass(BoxToHbaseReducer2.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }
//            PathUtil.deletePath(this.getConf(),inputPath);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

}
