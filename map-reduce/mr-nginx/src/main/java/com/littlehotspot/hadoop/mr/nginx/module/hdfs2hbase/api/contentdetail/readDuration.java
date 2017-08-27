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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.contentdetail;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
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
public class readDuration extends Configured implements Tool {

    private String contents;

    private String categorys;


    private static class MobileMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String row = Bytes.toString(result.getRow());
                String contentId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("content_id")));
                String categoryId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("category_id")));
                String commonValue = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("common_value")));
                String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
                String optionType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type")));
                String uuid = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("uuid")));
                String mediaType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("media_type")));
                String date = stampToDate(timestamps);
                SourceBean sourceBean = new SourceBean();
                sourceBean.setUuid(uuid);
                sourceBean.setContentId(contentId);
                sourceBean.setCategoryId(categoryId);
                sourceBean.setCommonValue(commonValue);
                sourceBean.setTimestamps(timestamps);
                sourceBean.setOptionType(optionType);
                sourceBean.setMediaType(mediaType);
                sourceBean.setDateTime(date);

                if (!StringUtils.isBlank(contentId)){
                    context.write(new Text(contentId+"|"+categoryId+"|"+uuid), new Text(sourceBean.rowLine1()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public static String stampToDate(String s){
            String res;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            return res;
        }
    }

    private static class Combiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Text> iterator = value.iterator();
                SourceBean bean = new SourceBean();
                String strat=null;
                String end=null;
                Long duration = 0l;
                while (iterator.hasNext()){
                    Text item = iterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher matcher =CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(rowLineContent);
                    if (!matcher.find()){
                        return;
                    }
                    bean.setContentId(matcher.group(2));
                    bean.setCategoryId(matcher.group(3));
                    bean.setDateTime(matcher.group(8));
                    if (!StringUtils.isBlank(matcher.group(4))){
                        if (matcher.group(4).equals("start")){
                            strat=matcher.group(6);
                        }else if (matcher.group(4).equals("end")){
                            end=matcher.group(6);
                        }
                    }


                }
                if (!StringUtils.isBlank(strat)&&!StringUtils.isBlank(end)){
                    duration=Long.valueOf(end) -Long.valueOf(strat);
                }
                context.write(new Text(bean.getContentId()+"|"+bean.getCategoryId()+"|"+bean.getDateTime()), new Text(bean.getContentId()+","+bean.getCategoryId()+","+bean.getDateTime()+","+duration));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {

                Configuration conf = context.getConfiguration();
                Iterator<Text> textIterator = value.iterator();
                SourceBean bean = new SourceBean();
                Integer duration=0;
                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher matcher =CommonVariables.MAPPER_FORMAT_REGEX2.matcher(rowLineContent);
                    if (!matcher.find()){
                        return;
                    }
                    bean.setContentId(matcher.group(1));
                    bean.setCategoryId(matcher.group(2));
                    bean.setDateTime(matcher.group(3));
                    duration += Integer.parseInt(matcher.group(4));
                    
                }
                bean.setReadDuration(duration.toString());
                context.write(new Text(bean.rowLine6()), new Text());

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
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);
            String time = CommonVariables.getParameterValue(Argument.Time);


            Job job = Job.getInstance(this.getConf(), readDuration.class.getSimpleName());
            job.setJarByClass(readDuration.class);

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

            //设置过滤器
            List<Filter> filters= new ArrayList<Filter>();

            SingleColumnValueFilter medfilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("media_type"), CompareFilter.CompareOp.EQUAL,Bytes.toBytes("content"));
            if (!StringUtils.isBlank(time)){
                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(time+"00"));
                filters.add(timefilter);
            }

            if(null==scan) {
                System.out.println("error : scan = null");
                return 1;
            }

            filters.add(medfilter);

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
            TableMapReduceUtil.initTableMapperJob("mobile_log", scan, MobileMapper.class, Text.class, Text.class, job,false);

            job.setCombinerClass(readDuration.Combiner.class);
            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
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

    public static boolean isYesterday(long time) {
        boolean isYesterday = false;
        Date date;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            date = sdf.parse(sdf.format(new Date()));
            if (time < date.getTime() && time > (date.getTime() - 24*60*60*1000)) {
                isYesterday = true;
            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return isYesterday;
    }

}
