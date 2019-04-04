/**
 * Copyright (c) 2019, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.phonebills
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 14:51
 */
package com.littlehotspot.hadoop.mr.phonebills;

import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2019年04月02日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class PhoneBillsScheduler extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Jar Clean action configuration");
        System.out.println("=================================================================");

        PhoneBillsConstant.CommonVariables.initMapReduce(this.getConf(), args);// 解析参数并初始化 MAP REDUCE

        // 工作流名称
        String jobName = ArgumentFactory.getParameterValue(Argument.JobName);
        ArgumentFactory.printInputArgument(Argument.JobName, jobName, false);

        // Hdfs 读取路径
        String[] hdfsInputPathArray = ArgumentFactory.getParameterValues(Argument.InputPath);
        ArgumentFactory.printInputArgument(Argument.InputPath, hdfsInputPathArray);

        // Hdfs 读取路径正则表示
        String hdfsInputPathRegex = ArgumentFactory.getParameterValue(Argument.InputPathReg);
        ArgumentFactory.printInputArgument(Argument.InputPathReg, hdfsInputPathRegex, false);

        // Hdfs 写入路径
        String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);
        ArgumentFactory.printInputArgument(Argument.OutputPath, hdfsOutputPath, false);


        // 准备工作
        if (hdfsInputPathArray == null && hdfsInputPathRegex == null) {
            String exceptionMessage = "The argument[\'" + Argument.InputPath.getName() + "\' OR \'" + Argument.InputPathReg.getName() + "\'] for this program is null";
            throw new IllegalArgumentException(exceptionMessage);
        }
        ArgumentFactory.checkNullValueForArgument(Argument.OutputPath, hdfsOutputPath);
        if (StringUtils.isBlank(jobName)) {
            jobName = this.getClass().getName();
        }

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Clean job task now >>>");
        System.out.println();
        System.out.flush();

//        // Map side tuning
//        this.setMapperConfig();
//
//        // Reduce side tuning
//        this.setReduceConfig();

//        this.getConf().setLong("mapreduce.input.fileinputformat.split.maxsize", BYTES_5M);

        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(this.getClass());


        // 作业输入
        if (hdfsInputPathArray == null || hdfsInputPathArray.length < 1) {
            FileInputFormat.setInputPaths(job, hdfsInputPathRegex);
        } else {
            for (String hdfsInputPath : hdfsInputPathArray) {
                Path inputPath = new Path(hdfsInputPath);
                FileInputFormat.addInputPath(job, inputPath);
            }
        }
//        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setMapperClass(PhoneBillsMapper.class);
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
            String exceptionMessage = String.format("MapReduce job[%s] execute failed", jobName);
            throw new RuntimeException(exceptionMessage);
        }
        return 0;
    }
}
