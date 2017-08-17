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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.readcount;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * 手机日志
 */
public class ReadCount extends Configured implements Tool {


    private static class MobileMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            String row = Bytes.toString(result.getRow());
            String conId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("con_id")));
            String conName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("con_name")));

            try {
                if (!StringUtils.isBlank(conId)&&!StringUtils.isBlank(conName)){
                    context.write(new Text(conId), new Text(conName));
                }else {
                    return;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {

        private HBaseHelper hBaseHelper;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();


        }
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {

                Configuration conf = context.getConfiguration();
                Iterator<Text> textIterator = value.iterator();
                StringBuffer rowLine = new StringBuffer();
                Integer count =0;
                String conName=null;
                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    conName = item.toString();

                    count++;

                }

                rowLine.append(key.toString()).append(",");
                rowLine.append(conName).append(",");
                rowLine.append(count.toString());

                context.write(new Text(rowLine.toString()), new Text());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariable.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hbaseSharePath = CommonVariable.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = CommonVariable.getParameterValue(Argument.HDFSCluster);
            String hdfsOutputPath = CommonVariable.getParameterValue(Argument.OutputPath);
            String time = CommonVariable.getParameterValue(Argument.Time);

            Job job = Job.getInstance(this.getConf(), ReadCount.class.getSimpleName());
            job.setJarByClass(ReadCount.class);

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
                return 1;
            }
            //设置过滤器
            if (!StringUtils.isBlank(time)){
                List<Filter> filters= new ArrayList<Filter>();
                SingleColumnValueFilter smallfilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("start"), CompareFilter.CompareOp.GREATER_OR_EQUAL,new BinaryComparator(Bytes.toBytes(time)));
                filters.add(smallfilter);
                FilterList filterList = new FilterList(filters);
                scan.setFilter(filterList);
            }

            TableMapReduceUtil.initTableMapperJob("user_read", scan, MobileMapper.class, Text.class, Text.class, job,false);


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

}
