/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.mapper
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:33
 */
package com.littlehotspot.hadoop.mr.hdfs.mapper;

import com.littlehotspot.hadoop.mr.hdfs.util.CleanByRegexConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>Mapper - 利用正则表达式清洗 HDFS 文件</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年06月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class CleanByRegexMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Pattern mapperInputFormatRegexPattern;

    /**
     * 机顶盒日志第一次清洗 Mapper
     *
     * @param key     输入键
     * @param value   输入值
     * @param context Mapper 上下文
     * @throws IOException          输入输出异常
     * @throws InterruptedException 中断异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String msg = value.toString();
            Matcher matcher = this.mapperInputFormatRegexPattern.matcher(msg);
            if (!matcher.find()) {
                return;
            }
            context.write(value,NullWritable.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);

        //从全局配置获取配置参数
        Configuration conf = context.getConfiguration();
        this.mapperInputFormatRegexPattern = conf.getPattern(CleanByRegexConstant.HadoopConfig.Key.MAPPER_INPUT_FORMAT_REGEX_PATTERN, null);
    }
}
