package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;

/**
 * <h1> 用户行为处理 </h1>
 * Created by Administrator on 2017-06-26 上午 11:02.
 */
public class SadActScheduler extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
//            CommonVariables.hBaseHelper = new HBaseHelper(this.getConf());
            // 获取参数
            String hdfsInputPath = CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);
            String sadActType = CommonVariables.getParameterValue(Argument.SadActType);
            this.getConf().set("sadActType", sadActType);

            Path inputPath = new Path(hdfsInputPath);
            Path outputPath = new Path(hdfsOutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getName());
            job.setJarByClass(this.getClass());

            job.setMapperClass(SadActMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setCombinerClass(SadActCombiner.class);

            job.setReducerClass(SadActReducer.class);
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