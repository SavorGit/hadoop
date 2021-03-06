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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 手机日志
 */
public class InToHbase extends Configured implements Tool {


    private static class MobileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private static final Pattern PATTERN = Pattern.compile("^(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String line = value.toString();
                Matcher matcher = PATTERN.matcher(line);
                if (!matcher.find()) {
                    return;
                }
                long version = System.currentTimeMillis();
                String familyName = "attr";
                byte[] rowKeyBytes = Bytes.toBytes(matcher.group(1));
                Put put = new Put(rowKeyBytes);// 设置rowkey

                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("area_id"), version, Bytes.toBytes(matcher.group(2)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("area_name"), version, Bytes.toBytes(matcher.group(3)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_id"), version, Bytes.toBytes(matcher.group(4)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_name"), version, Bytes.toBytes(matcher.group(5)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("room_id"), version, Bytes.toBytes(matcher.group(6)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("room_name"), version, Bytes.toBytes(matcher.group(7)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("box_id"), version, Bytes.toBytes(matcher.group(8)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("box_name"), version, Bytes.toBytes(matcher.group(9)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mac"), version, Bytes.toBytes(matcher.group(10)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("media_id"), version, Bytes.toBytes(matcher.group(11)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("media_name"), version, Bytes.toBytes(matcher.group(12)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("play_count"), version, Bytes.toBytes(matcher.group(13)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("play_time"), version, Bytes.toBytes(matcher.group(14)));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("play_date"), version, Bytes.toBytes(StringUtils.trim(matcher.group(15))));


                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
                context.write(rowKey, put);
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
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsInputPath = CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), InToHbase.class.getSimpleName());
            job.setJarByClass(InToHbase.class);

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
            }//

            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            HTable hTable = new HTable(this.getConf(), "media_sta");

            FileOutputFormat.setOutputPath(job, outputPath);
            job.setMapperClass(MobileMapper.class);
            job.setReducerClass(KeyValueSortReducer.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setPartitionerClass(SimpleTotalOrderPartitioner.class);


            HFileOutputFormat2.configureIncrementalLoad(job, hTable, hTable.getRegionLocator());

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }

            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(this.getConf());
            loader.doBulkLoad(outputPath, hTable);

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

}
