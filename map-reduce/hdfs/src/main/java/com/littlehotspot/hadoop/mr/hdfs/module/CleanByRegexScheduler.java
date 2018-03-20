/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.module
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:32
 */
package com.littlehotspot.hadoop.mr.hdfs.module;

import com.littlehotspot.hadoop.mr.hdfs.mapper.CleanByRegexMapper;
import com.littlehotspot.hadoop.mr.hdfs.util.CleanByRegexConstant;
import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;
import java.util.regex.Pattern;

/**
 * <h1>调度器 - 利用正则表达式清洗 HDFS 文件</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class CleanByRegexScheduler extends Configured implements Tool {

    private static final int BYTES_5M = 5 * 1024 * 1024;
    private static final int BYTES_10M = 10 * 1024 * 1024;
    private static final int BYTES_64M = 128 * 1024 * 1024;
    private static final int BYTES_128M = 128 * 1024 * 1024;
    private static final int BYTES_256M = 256 * 1024 * 1024;

    /**
     * 运行
     *
     * @param args 参数列表。参数名：
     *             jobName      任务名称(可选)
     *             hdfsCluster  Hdfs 集群地址(可选)
     *             inRegex      输入 Mapper 的正则表达式
     *             hdfsIn       输入的 HDFS 路径
     *             hdfsOut      输出的 HDFS 路径
     * @return 返回运行结果
     * @throws Exception 异常
     */
    @Override
    public int run(String[] args) throws Exception {
//        try {
        CleanByRegexConstant.CommonVariables.initMapReduce(this.getConf(), args);// 解析参数并初始化 MAP REDUCE

        // Mapper 输入正则表达式
        String jobName = ArgumentFactory.getParameterValue(Argument.JobName);
        ArgumentFactory.printInputArgument(Argument.JobName, jobName, false);

        // Mapper 输入正则表达式
        String matcherRegex = ArgumentFactory.getParameterValue(Argument.MapperInputFormatRegex);
        ArgumentFactory.printInputArgument(Argument.MapperInputFormatRegex, matcherRegex, false);

        // Hdfs 读取路径
        String[] hdfsInputPathArray = ArgumentFactory.getParameterValues(Argument.InputPath);
        ArgumentFactory.printInputArgument(Argument.InputPath, hdfsInputPathArray);

        // Hdfs 写入路径
        String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);
        ArgumentFactory.printInputArgument(Argument.OutputPath, hdfsOutputPath, false);


        // 准备工作
        ArgumentFactory.checkNullValueForArgument(Argument.MapperInputFormatRegex, matcherRegex);
        ArgumentFactory.checkNullValuesForArgument(Argument.InputPath, hdfsInputPathArray);
        ArgumentFactory.checkNullValueForArgument(Argument.OutputPath, hdfsOutputPath);
        if (StringUtils.isBlank(jobName)) {
            jobName = this.getClass().getName();
        }

        // Map side tuning
        this.setMapperConfig();

        // Reduce side tuning
        this.setReduceConfig();


        this.getConf().setLong("mapreduce.input.fileinputformat.split.maxsize", BYTES_5M);
        this.getConf().setPattern(CleanByRegexConstant.HadoopConfig.Key.MAPPER_INPUT_FORMAT_REGEX_PATTERN, Pattern.compile(matcherRegex));// 配置 Mapper 输入的正则匹配对象
//            System.out.println(this.getConf().get("mapreduce.input.fileinputformat.split.maxsize"));
//            System.out.println(this.getConf().get("mapreduce.job.inputformat.class"));

        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(this.getClass());


        // 作业输入
        for (String hdfsInputPath : hdfsInputPathArray) {
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);
        }
        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setMapperClass(CleanByRegexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 作业输出
        Path outputPath = new Path(hdfsOutputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
//            job.setReducerClass(GeneralReducer.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);

        // 如果输入路径已经存在，则删除
        FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        boolean status = job.waitForCompletion(true);
        if (!status) {
            String exceptionMessage = String.format("MapReduce[Clean-By-Regex] task[%s] execute failed", jobName);
            throw new RuntimeException(exceptionMessage);
        }
        return 0;
//        } catch (Exception e) {
//            e.printStackTrace();
//            return 1;
//        }
    }

    // Reduce side tuning
    private void setReduceConfig() {
        // Reduce Shuffle
        this.getConf().setInt("mapred.reduce.parallel.copies", 10);
        this.getConf().setInt("mapred.reduce.copy.backoff", 300);

        // Reduce Merge
        this.getConf().setDouble("mapred.job.shuffle.input.buffer.percent", 0.7);
        this.getConf().setDouble("mapred.job.shuffle.merge.percent", 0.66);
        this.getConf().set("mapred.child.Java.opts", "-Xmx1024m");

        // Reduce Reduce
        this.getConf().setDouble("mapred.job.reduce.input.buffer.percent", 0.0);

//        this.getConf().setLong("mapred.reduce.tasks", 1);
//        this.getConf().setLong("mapred.tasktracker.reduce.tasks.maximum", 2);
        this.getConf().setDouble("mapred.reduce.slowstart.completed.maps", 0.05);
    }

    // Map side tuning
    private void setMapperConfig() {
        // Mapper InputFormat
        this.getConf().setInt("mapred.min.split.size", BYTES_64M);
        this.getConf().setInt("mapred.max.split.size", BYTES_128M);
//            this.getConf().setLong("dfs.block.size", BYTES_128M);

        // Mapper Buffer
//        this.getConf().setInt("io.sort.mb", BYTES_256M);
////            this.getConf().setLong("mapreduce.task.io.sort.mb", BYTES_256M);
        this.getConf().setDouble("io.sort.spill.percent", 0.8);

        // Mapper Merge
        this.getConf().setLong("io.sort.factor", 64);
//            this.getConf().setLong("mapreduce.task.io.sort.factor", 64);

        // Mapper Combine
        this.getConf().setLong("min.num.spill.for.combine", 3);

//        // Mapper 输出中间数据到磁盘
//        this.getConf().setBoolean("mapred.compress.map.output", true);
//        this.getConf().set("mapred.map.output.compression.codec", "GzipCodec");


//        this.getConf().setLong("mapred.tasktracker.map.tasks.maximum", 2);
    }
}
