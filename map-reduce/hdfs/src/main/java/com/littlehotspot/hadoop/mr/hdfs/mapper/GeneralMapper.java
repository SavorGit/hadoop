/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hdfs.mapper
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 16:06
 */
package com.littlehotspot.hadoop.mr.hdfs.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <h1>Mapper - 通用</h1>
 * 输出 KEY 为文件内容的每一行
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class GeneralMapper extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * 通用 Mapper
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
            context.write(value, new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
