/**
 * Copyright (c) 2019, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.phonebills
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 14:55
 */
package com.littlehotspot.hadoop.mr.phonebills;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2019年04月02日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class PhoneBillsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Pattern _35FieldInclusionsPattern;

    private Pattern _36FieldInclusionsPattern;

    private Pattern _37FieldInclusionsPattern;

    private Pattern _serviceKeyExclusionsPattern;

    private Pattern _splitAreaPhonePattern;

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
        String msg = value.toString(), row;
//        System.out.println("1==============" + msg);
        if (_serviceKeyExclusionsPattern.matcher(msg).find()) {
            System.out.println("return _serviceKeyExclusionsPattern");
            return;
        }
//        System.out.println("2==============" + msg);
        Matcher _splitAreaPhoneMatcher = _splitAreaPhonePattern.matcher(msg);
        if (_splitAreaPhoneMatcher.find()) {
//            System.out.println(_splitAreaPhoneMatcher.group(1) + "        " + _splitAreaPhoneMatcher.group(2));
            row = _splitAreaPhoneMatcher.group(1) + "-" + _splitAreaPhoneMatcher.group(2);
        } else {
            row = msg;
        }
//        System.out.println("3==============" + row);
        if (_35FieldInclusionsPattern.matcher(row).find()) {
            row += "||";
        } else if (_36FieldInclusionsPattern.matcher(row).find()) {
            row += "|";
        } else if (!_37FieldInclusionsPattern.matcher(row).find()) {
            System.out.println("return !_37FieldInclusionsPattern");
            return;
        }
//        System.out.println("4==============" + row);
        while (row.contains("|")) {
            row = row.replace('|', ',');
        }
//        System.out.println("row=" + row);
        context.write(new Text(row), NullWritable.get());
//        System.out.println();
//        System.out.println();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
//        super.setup(context);

        _serviceKeyExclusionsPattern = Pattern.compile("^[^|]*\\|(?:900003)|(?:910003)|(?:900004)|(?:910004)\\|");
        _35FieldInclusionsPattern = Pattern.compile("^[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*$");
        _36FieldInclusionsPattern = Pattern.compile("^[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*$");
        _37FieldInclusionsPattern = Pattern.compile("^[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*\\|[^|]*$");
        _splitAreaPhonePattern = Pattern.compile("^([^|]*\\|[^|]*\\|[^|]*\\|0(?:(?:[12][0-9])|(?:[3456789][0-9]{2})))([^|]+\\|.+)$");

        //从全局配置获取配置参数
        Configuration conf = context.getConfiguration();
    }
}
