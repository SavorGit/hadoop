package com.littlehotspot.hadoop.mr.user.tag.hbase;

import com.littlehotspot.hadoop.mr.user.tag.util.ArgumentUtil;
import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.commons.lang.StringUtils;
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
 * <h1> title </h1>
 * Created by Administrator on 2017-08-09 上午 11:03.
 */
public class UserTagScheduler extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {

            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);

            String hdfsIn = ArgumentFactory.getParameterValue(Argument.InputPath);
            ArgumentFactory.printInputArgument(Argument.InputPath, hdfsIn, false);

            String hdfsOut = ArgumentFactory.getParameterValue(Argument.OutputPath);
            ArgumentFactory.printInputArgument(Argument.OutputPath, hdfsOut, false);

            Path inputPath = new Path(hdfsIn);
            Path outputPath = new Path(hdfsOut);

            Job job = Job.getInstance(this.getConf(), this.getClass().getName());
            job.setJarByClass(this.getClass());

            job.setMapperClass(UserTagMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(UserTagReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPaths(job, hdfsIn);
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
