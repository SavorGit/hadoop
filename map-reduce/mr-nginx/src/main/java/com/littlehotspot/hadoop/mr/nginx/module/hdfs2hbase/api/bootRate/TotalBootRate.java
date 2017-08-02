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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * <h1>调度器 - 用户 [API]</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TotalBootRate extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

//            String hbaseRoot = CommonVariables.getParameterValue(Argument.HbaseRoot);
//            String hbaseZoo = CommonVariables.getParameterValue(Argument.HbaseZookeeper);
//            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
//
//            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            String columnFamily = CommonVariables.getParameterValue(Argument.ColumnFamily);
            String columnName = CommonVariables.getParameterValue(Argument.ColumnName);

            Configuration conf = new Configuration();

//            HTable hTable = new HTable(this.getConf(),"box_log");
//            Job job = new Job(hTable.getConfiguration(),this.getClass().getName());
            Job job = Job.getInstance(this.getConf(), TotalBootRate.class.getSimpleName());
            job.setJarByClass(TotalBootRate.class);

            // 避免报错：ClassNotFoundError hbaseConfiguration
//            Configuration jobConf = job.getConfiguration();
//            FileSystem hdfs = FileSystem.get(new URI(hdfsCluster), jobConf);
//            if (StringUtils.isNotBlank(hbaseSharePath)) {
//                Path hBaseSharePath = new Path(hbaseSharePath);
//                FileStatus[] hBaseShareJars = hdfs.listStatus(hBaseSharePath);
//                for (FileStatus fileStatus : hBaseShareJars) {
//                    if (!fileStatus.isFile()) {
//                        continue;
//                    }
//                    Path archive = fileStatus.getPath();
//                    FileSystem fs = archive.getFileSystem(jobConf);
//                    DistributedCache.addArchiveToClassPath(archive, jobConf, fs);
//                }//
//            }
            Scan scan = new Scan();
//            scan.setCaching(500);
//            scan.setCacheBlocks(false);
//            scan.addColumn(Bytes.toBytes("attr"),Bytes.toBytes("mda_type"));
//            scan.addColumn(Bytes.toBytes("attr"),Bytes.toBytes("option_type"));

            if(null==scan) {
                System.out.println("error : scan = null");
                System.exit(1);
            }


            TableMapReduceUtil.initTableMapperJob("boot_rate", scan, BoxTableMapper.class, Text.class, Text.class, job,false);


            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(BoxTableReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//            job.setNumReduceTasks(0);
            // 执行任务
            boolean state = job.waitForCompletion(true);
            if (!state) {
                throw new Exception("MapReduce task execute failed.........");
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

}