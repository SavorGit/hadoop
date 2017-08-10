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
package com.littlehotspot.hadoop.mr.user.tag.hbase;

import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 为用户打标签
 */
public class TagIntoHbaseMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IOException("please write input path and output path...");
        }

        Locale.setDefault(Locale.CHINA);
        Configuration conf = new Configuration();

        ToolRunner.run(conf, new TagIntoHbaseMain(), args);
    }

    private static class TagMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private static final Pattern PATTERN = Pattern.compile("^(.*)\\|(\\d*)$");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String line = value.toString().trim();
                Matcher matcher = PATTERN.matcher(line);
                if (!matcher.find()) {
                    return;
                }
                long version = System.currentTimeMillis();
                String familyName = "acti";
                byte[] rowKeyBytes = Bytes.toBytes(matcher.group(1));
                Put put = new Put(rowKeyBytes);// 设置rowkey

                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("user_type"), version, Bytes.toBytes(matcher.group(2)));

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

            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hdfsInputPath = ArgumentFactory.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), TagIntoHbaseMain.class.getSimpleName());
            job.setJarByClass(TagIntoHbaseMain.class);

            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            HTable hTable = new HTable(this.getConf(), "user_basic");

            FileOutputFormat.setOutputPath(job, outputPath);
            job.setMapperClass(TagMapper.class);
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
