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
import com.littlehotspot.hadoop.mr.hdfs.reducer.GeneralReducer;
import com.littlehotspot.hadoop.mr.hdfs.util.Argument;
import com.littlehotspot.hadoop.mr.hdfs.util.CleanByRegexConstant;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

    /**
     * 运行
     *
     * @param args 参数列表。参数名：
     *             hdfsCluster Hdfs 集群地址
     *             hdfsIn      输入的 HDFS 路径
     *             hdfsOut     输出的 HDFS 路径
     * @return 返回运行结果
     * @throws Exception 异常
     */
    @Override
    public int run(String[] args) throws Exception {
        CleanByRegexConstant.CommonVariables.initMapReduce(this.getConf(), args);// 解析参数并初始化 MAP REDUCE

        // Mapper 输入正则表达式
        String matcherRegex = CleanByRegexConstant.CommonVariables.getParameterValue(Argument.MapperInputFormatRegex);
        if (matcherRegex == null) {
            throw new IllegalArgumentException("The argument['inRegex'] for this program is null");
        }

        // Hdfs 读取路径
        String hdfsInputPath = CleanByRegexConstant.CommonVariables.getParameterValue(Argument.InputPath);
        if (hdfsInputPath == null) {
            throw new IllegalArgumentException("The argument['hdfsIn'] for this program is null");
        }

        // Hdfs 写入路径
        String hdfsOutputPath = CleanByRegexConstant.CommonVariables.getParameterValue(Argument.OutputPath);
        if (hdfsOutputPath == null) {
            throw new IllegalArgumentException("The argument['hdfsOut'] for this program is null");
        }


        CleanByRegexConstant.MAPPER_INPUT_FORMAT_REGEX = Pattern.compile(matcherRegex);// 生成正则匹配对象

        Path inputPath = new Path(hdfsInputPath);
        Path outputPath = new Path(hdfsOutputPath);

        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 作业输入
        job.setMapperClass(CleanByRegexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 作业输出
        job.setReducerClass(GeneralReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 如果输入路径已经存在，则删除
        FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        boolean status = job.waitForCompletion(true);
        if (!status) {
            String exceptionMessage = String.format("MapReduce[clean-by-regex()] task execute failed", matcherRegex);
            throw new RuntimeException(exceptionMessage);
        }
        return 0;
    }
}
