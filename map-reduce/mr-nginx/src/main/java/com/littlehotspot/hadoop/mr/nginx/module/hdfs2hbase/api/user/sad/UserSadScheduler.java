package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
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
 * <h1> 用户行为最终处理 </h1>
 * Created by Administrator on 2017-06-27 下午 3:05.
 */
public class UserSadScheduler extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
            CommonVariables.hBaseHelper = new HBaseHelper(this.getConf());
            // 获取参数
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);

            String hdfsInputStart = CommonVariables.getParameterValue(Argument.InputPathStart);
            String hdfsInputEnd = CommonVariables.getParameterValue(Argument.InputPathEnd);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            String sadType = CommonVariables.getParameterValue(Argument.SadType);
            String hbaseRoot = CommonVariables.getParameterValue(Argument.HbaseRoot);
            String hbaseZoo = CommonVariables.getParameterValue(Argument.HbaseZookeeper);
            this.getConf().set("sadType", sadType);
            this.getConf().set("hbase.rootdir", hbaseRoot);
            this.getConf().set("hbase.zookeeper.quorum", hbaseZoo);

            Path inputPath = new Path(hdfsInputStart);
            Path inputPath1 = new Path(hdfsInputEnd);
            Path outputPath = new Path(hdfsOutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getName());
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
            }//

            job.setMapperClass(UserSadMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(UserSadReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileInputFormat.addInputPath(job, inputPath1);
            FileOutputFormat.setOutputPath(job, outputPath);

            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

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