/**
 * Copyright (c) 2019, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.module
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:21
 */
package com.littlehotspot.hadoop.mr.hdfs.module;

import com.littlehotspot.hadoop.mr.hdfs.mapper.ConvertByRegexMapper;
import com.littlehotspot.hadoop.mr.hdfs.util.CleanByRegexConstant;
import com.littlehotspot.hadoop.mr.hdfs.util.ConvertByRegexConstant;
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
import java.util.regex.Pattern;

/**
 * <h1>调度器 - 利用正则表达式转换 HDFS 文件内容</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2019年05月17日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ConvertByRegexScheduler extends Configured implements Tool {

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

        System.out.println();
        System.out.println("Jar Convert action configuration");
        System.out.println("=================================================================");

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

        // Hdfs 读取路径正则表示
        String hdfsInputPathRegex = ArgumentFactory.getParameterValue(Argument.InputPathReg);
        ArgumentFactory.printInputArgument(Argument.InputPathReg, hdfsInputPathRegex, false);

        // Hdfs 写入路径
        String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);
        ArgumentFactory.printInputArgument(Argument.OutputPath, hdfsOutputPath, false);


        // 准备工作
        ArgumentFactory.checkNullValueForArgument(Argument.MapperInputFormatRegex, matcherRegex);
        if (hdfsInputPathArray == null && hdfsInputPathRegex == null) {
            String exceptionMessage = "The argument['" + Argument.InputPath.getName() + "' OR '" + Argument.InputPathReg.getName() + "'] for this program is null";
            throw new IllegalArgumentException(exceptionMessage);
        }
        ArgumentFactory.checkNullValueForArgument(Argument.OutputPath, hdfsOutputPath);
        if (StringUtils.isBlank(jobName)) {
            jobName = this.getClass().getName();
        }

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Convert job task now >>>");
        System.out.println();
        System.out.flush();

        this.getConf().setPattern(ConvertByRegexConstant.HadoopConfig.Key.MAPPER_INPUT_FORMAT_REGEX_PATTERN, Pattern.compile(matcherRegex));// 配置 Mapper 输入的正则匹配对象

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
        job.setMapperClass(ConvertByRegexMapper.class);
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
            String exceptionMessage = String.format("MapReduce[Convert-By-Regex] task[%s] execute failed", jobName);
            throw new RuntimeException(exceptionMessage);
        }
        return 0;
    }
}
