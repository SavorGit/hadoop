/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.mobile
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 17:44
 */
package com.littlehotspot.hadoop.mr.nginx.mobile;

import com.littlehotspot.hadoop.mr.nginx.mobile.util.ArgumentUtil;
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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年05月19日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class NginxLog extends Configured implements Tool {

    //    private static Pattern NGINX_LOG_FORMAT_REGEX = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) - - \\[(.+)\\] ([A-Z]+) ([^ ]+) HTTP/[^ ]+ \"(\\d{3})\" \\d+ \"(.+)\" \"(.+)\" \"(.+)\"$");
    public static Pattern MAPPER_INPUT_FORMAT_REGEX = Pattern.compile("^(\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}\\.\\d{0,3}) - [^ ]+ \\[(.+)\\] ([A-Z]+) ([^ ]+) HTTP/[^ ]+ \"(\\d{3})\" \\d+ \"(.+)\" \"([^-]+.*)\" \"(.+)\" \"(.+)\"$");

    private static Map<String, List<String>> parameters;

    @Override
    public int run(String[] args) throws Exception {
        try {
            this.analysisArgument(args);

            String matcherRegex = ArgumentUtil.getParameterValue(parameters, Argument.MapperInputFormatRegex.getName(), Argument.MapperInputFormatRegex.getDefaultValue());
            String hdfsInputPath = ArgumentUtil.getParameterValue(parameters, Argument.InputPath.getName(), Argument.InputPath.getDefaultValue());
            String hdfsOutputPath = ArgumentUtil.getParameterValue(parameters, Argument.OutputPath.getName(), Argument.OutputPath.getDefaultValue());

            // 配置数据格式
            if (StringUtils.isNotBlank(matcherRegex)) {
                MAPPER_INPUT_FORMAT_REGEX = Pattern.compile(matcherRegex);
            }

            Path inputPath = new Path(hdfsInputPath);
            Path outputPath = new Path(hdfsOutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setMapperClass(NginxMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(NginxReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), new Configuration());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

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

    private void analysisArgument(String[] args) {
        parameters = ArgumentUtil.analysisArgument(args);
    }
}
