/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.hfile
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:33
 */
package com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.hfile;

import com.littlehotspot.hadoop.mr.nginx.mobile.hdfs2hbase.Argument;
import com.littlehotspot.hadoop.mr.nginx.util.ArgumentUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * <h1>调度器 - 通过 HFile 方式把 HDFS 数据导入到 HBase</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月24日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class HFileScheduler extends Configured implements Tool {

    public static Pattern MAPPER_INPUT_FORMAT_REGEX = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    private static Map<String, List<String>> parameters;

    @Override
    public int run(String[] args) throws Exception {
        try {
            this.analysisArgument(args);// 解析参数

            // 获取参数
            String hbaseTableName = ArgumentUtil.getParameterValue(parameters, Argument.HbaseTable.getName(), Argument.HbaseTable.getDefaultValue());
            String hdfsInputPath = ArgumentUtil.getParameterValue(parameters, Argument.InputPath.getName(), Argument.InputPath.getDefaultValue());
            String hdfsOutputPath = ArgumentUtil.getParameterValue(parameters, Argument.OutputPath.getName(), Argument.OutputPath.getDefaultValue());

            Path inputPath = new Path(hdfsInputPath);
            Path outputPath = new Path(hdfsOutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(HFileMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setOutputFormatClass(HFileOutputFormat2.class);
//            job.setReducerClass(PutSortReducer.class);
            job.setReducerClass(PutSortReducer.class);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputKeyClass(KeyValue.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            // 生成HFile
            Connection conn = ConnectionFactory.createConnection(this.getConf());
            HTable table = (HTable) conn.getTable(TableName.valueOf(hbaseTableName));
            HFileOutputFormat2.configureIncrementalLoad(job, table);

            // 执行任务
            boolean state = job.waitForCompletion(true);
            if (!state) {
                throw new IOException("error a job,hdfs to hbase import failed!");
            }

            // 将HFile文件导入HBase
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(this.getConf());
            loader.doBulkLoad(outputPath, table);
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    private void analysisArgument(String[] args) {
        parameters = ArgumentUtil.analysisArgument(args);
    }
}
