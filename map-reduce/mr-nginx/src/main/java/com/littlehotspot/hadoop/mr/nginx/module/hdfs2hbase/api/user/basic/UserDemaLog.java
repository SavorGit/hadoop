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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.CommonVariables;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.NgxSrcUserBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.UserActBean;
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
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class UserDemaLog extends Configured implements Tool {

    private static class MobileMapper extends TableMapper<Text, Text> {


        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Mapper.Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String row = Bytes.toString(result.getRow());
                String mobile_id = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mobile_id")));
                String opt = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type")));
                String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
                if (StringUtils.isBlank(mobile_id)){
                    return;
                }
                context.write(new Text(mobile_id), new Text(mobile_id+","+timestamps));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class Combiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Text> iterator = value.iterator();
                UserActBean userActBean = new UserActBean();
                Integer count=0;
                while (iterator.hasNext()){
                    Text item = iterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher matcher = CommonVariables.MAPPER_BOX_Hbase_FORMAT_REGEX.matcher(rowLineContent);
                    if (!matcher.find()) {
                        return;
                    }
                    userActBean.setDeviceId(matcher.group(1));
                    if (StringUtils.isBlank(userActBean.getTime())){
                        userActBean.setTime(matcher.group(2));
                    }else if (Long.valueOf(userActBean.getTime())>=Long.valueOf(matcher.group(2))){
                        userActBean.setTime(matcher.group(2));
                    }
                    count ++;
                }
                userActBean.setCount(count.toString());
                userActBean.setType("dema");
                context.write(new Text(userActBean.getDeviceId()), new Text(userActBean.rowLine()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Text> iterator = value.iterator();
                UserActBean userActBean = new UserActBean();
                while (iterator.hasNext()){
                    Text item = iterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher matcher = CommonVariables.MAPPER_ACT_FORMAT_REGEX.matcher(rowLineContent);
                    if (!matcher.find()) {
                        return;
                    }
                    userActBean.setDeviceId(matcher.group(1));
                    if (StringUtils.isBlank(userActBean.getTime())){
                        userActBean.setTime(matcher.group(2));
                    }else if (Long.valueOf(userActBean.getTime())>=Long.valueOf(matcher.group(2))){
                        userActBean.setTime(matcher.group(2));
                    }
                    if (StringUtils.isBlank(userActBean.getCount())){
                        userActBean.setCount(matcher.group(3));
                    }else {
                        Long count=Long.valueOf(userActBean.getCount())+Long.valueOf(matcher.group(3));
                        userActBean.setCount(count.toString());
                    }

                }
                userActBean.setType("dema");
                context.write(new Text(userActBean.rowLine()), new Text());
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
            String hbaseRoot = CommonVariables.getParameterValue(Argument.HbaseRoot);
            String hbaseZoo = CommonVariables.getParameterValue(Argument.HbaseZookeeper);
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
//
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            String startTime = CommonVariables.getParameterValue(Argument.StartTime);
            String endTime = CommonVariables.getParameterValue(Argument.EndTime);

            Job job = Job.getInstance(this.getConf(), UserDemaLog.class.getSimpleName());
            job.setJarByClass(UserDemaLog.class);

            // 避免报错：ClassNotFoundError hbaseConfiguration
            Configuration jobConf = job.getConfiguration();
            FileSystem hdfs = FileSystem.get(new URI(hdfsCluster), jobConf);
            if (StringUtils.isNotBlank(hbaseSharePath)) {
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
            }
            Scan scan = new Scan();

            List<Filter> filters= new ArrayList<Filter>();
//            RegexStringComparator comp = new RegexStringComparator("^\\s*&");
            SingleColumnValueFilter nullfilter=new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("mobile_id"),CompareFilter.CompareOp.NOT_EQUAL,new RegexStringComparator("^\\s*$"));
            SingleColumnValueFilter optfilter=new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("option_type"),CompareFilter.CompareOp.NOT_EQUAL,new RegexStringComparator("^(start)|(end)$"));
            SingleColumnValueFilter typefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("mda_type"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("vod"));

            if (!StringUtils.isBlank(startTime)) {
                String s = dateToStamp(startTime);
                SingleColumnValueFilter startfilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("timestamps"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(s));
                filters.add(startfilter);
            }
            if (!StringUtils.isBlank(endTime)) {
                String s = dateToStamp(startTime);
                SingleColumnValueFilter endfilter=new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("timestamps"),CompareFilter.CompareOp.LESS_OR_EQUAL,Bytes.toBytes(s));
                filters.add(endfilter);
            }

            if(null==scan) {
                System.out.println("error : scan = null");
                System.exit(1);
            }


            filters.add(nullfilter);
            filters.add(optfilter);
            filters.add(typefilter);

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);

            TableMapReduceUtil.initTableMapperJob("box_log", scan, MobileMapper.class, Text.class, Text.class, job,false);


            job.setCombinerClass(Combiner.class);

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
    /*
        * 将时间转换为时间戳
        */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        Date date = simpleDateFormat.parse(s+"00000");
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }

}
