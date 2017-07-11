package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.net.URI;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-06 下午 4:02.
 */
public class ResourceScheduler extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
//            CommonVariables.hBaseHelper = new HBaseHelper(this.getConf());

            String[] newArgs = new GenericOptionsParser(this.getConf(), args).getRemainingArgs();

            // 获取参数
            String hdfsInputPath = CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);
            String resourceType = CommonVariables.getParameterValue(Argument.ResourceType);
            this.getConf().set("resourceType", resourceType);

            Path inputPath = new Path(hdfsInputPath);
            Path outputPath = new Path(hdfsOutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getName());
            job.setJarByClass(this.getClass());

            Configuration jobConf = job.getConfiguration();


            FileSystem hdfs = FileSystem.get(new URI("hdfs://devpd1:8020"), jobConf);
            Path hBaseSharePath = new Path("/user/oozie/share/lib/lib_20170601134717/hbase");
            FileStatus[] hBaseShareJars = hdfs.listStatus(hBaseSharePath);
            for (FileStatus fileStatus : hBaseShareJars) {
                if (!fileStatus.isFile()) {
                    continue;
                }
                Path archive = fileStatus.getPath();
                FileSystem fs = archive.getFileSystem(jobConf);
                DistributedCache.addArchiveToClassPath(archive, jobConf, fs);
            }

            job.setMapperClass(ResourceMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(ResourceReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
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
