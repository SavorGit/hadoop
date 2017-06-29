/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box.module
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:32
 */
package com.littlehotspot.hadoop.mr.box.module;

import com.littlehotspot.hadoop.mr.box.mapper.LogClean1Mapper;
import com.littlehotspot.hadoop.mr.box.reducer.GeneralReducer;
import com.littlehotspot.hadoop.mr.box.util.Argument;
import com.littlehotspot.hadoop.mr.box.util.LogClean1Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
 * <h1>调度器 - 机顶盒日志第一次清洗</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class LogClean1Scheduler extends Configured implements Tool {

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
        try {
            LogClean1Constant.CommonVariables.initMapReduce(this.getConf(), args);// 解析参数并初始化 MAP REDUCE

            String matcherRegex = LogClean1Constant.CommonVariables.getParameterValue(Argument.MapperInputFormatRegex);
            String hdfsInputPath = LogClean1Constant.CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = LogClean1Constant.CommonVariables.getParameterValue(Argument.OutputPath);

            // 配置数据格式
            if (StringUtils.isNotBlank(matcherRegex)) {
                LogClean1Constant.MAPPER_INPUT_FORMAT_REGEX = Pattern.compile(matcherRegex);
            }

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);
            job.setMapperClass(LogClean1Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), new Configuration());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(GeneralReducer.class);
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
