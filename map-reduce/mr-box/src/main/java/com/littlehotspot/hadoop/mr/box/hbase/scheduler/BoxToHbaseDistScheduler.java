package com.littlehotspot.hadoop.mr.box.hbase.scheduler;

import com.littlehotspot.hadoop.mr.box.hbase.mapper.BoxToHbaseMapper;
import com.littlehotspot.hadoop.mr.box.common.Argument;
import com.littlehotspot.hadoop.mr.box.hbase.reducer.BoxToHbaseReducer;
import com.littlehotspot.hadoop.mr.box.util.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;

/**
 *@Author 刘飞飞
 *@Date 2017/7/24 15:16
 */
public class BoxToHbaseDistScheduler extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        try {
            Constant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
            // 获取参数
            String hbaseSharePath = Constant.CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = Constant.CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsInputPath = Constant.CommonVariables.getParameterValue(Argument.InputPath);

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
            FileInputFormat.setInputPaths(job, inputPath);
            job.setMapperClass(BoxToHbaseMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,"box_log");
            job.setOutputFormatClass(TableOutputFormat.class);
            job.setOutputKeyClass(ImmutableBytesWritable.class);
            job.setOutputValueClass(Put.class);
            job.setReducerClass(BoxToHbaseReducer.class);
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
