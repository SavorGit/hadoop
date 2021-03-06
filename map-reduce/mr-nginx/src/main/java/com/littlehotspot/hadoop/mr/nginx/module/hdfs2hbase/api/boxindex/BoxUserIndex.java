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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.boxindex;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate.BoxTableMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 手机日志
 */
public class BoxUserIndex extends Configured implements Tool {

    private static class MobileMapper extends TableMapper<Text, Text> {


        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String row = Bytes.toString(result.getRow());
                String mac = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac")));
                String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
                String mobileId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mobile_id")));
                if (!StringUtils.isBlank(mobileId)){
                    context.write(new Text(mac+","+timestamps+","+mobileId), new Text());
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
                String string = key.toString();
                context.write(key, new Text());
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
            String matcherRegex = CommonVariables.getParameterValue(Argument.MapperInputFormatRegex);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);
            String time = CommonVariables.getParameterValue(Argument.Time);

            Job job = Job.getInstance(this.getConf(), BoxUserIndex.class.getSimpleName());
            job.setJarByClass(BoxUserIndex.class);

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

            Scan scan = new Scan();

            if(null==scan) {
                System.out.println("error : scan = null");
                System.exit(1);
            }

            if (!StringUtils.isBlank(time)){
                List<Filter> filters= new ArrayList<Filter>();
                RegexStringComparator comp = new RegexStringComparator("^"+time);
                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.EQUAL,comp);

                filters.add(timefilter);
                FilterList filterList = new FilterList(filters);
                scan.setFilter(filterList);
            }

            TableMapReduceUtil.initTableMapperJob("box_log", scan, MobileMapper.class, Text.class, Text.class, job,false);

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

}
