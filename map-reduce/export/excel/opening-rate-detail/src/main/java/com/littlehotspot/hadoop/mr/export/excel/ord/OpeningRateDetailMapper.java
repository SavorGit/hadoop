/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 16:41
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>Mapper - 开机率明细</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月29日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class OpeningRateDetailMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Gson GSON = new Gson();
    private static Pattern PATTERN_CUT_DATA;

    private Map<String, ExcelConfigSheet> excelSheetConfigMap;
    private Text reduceKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        reduceKey.clear();
        String line = value.toString();
        Matcher cutDataMatcher = PATTERN_CUT_DATA.matcher(line);
        if (!cutDataMatcher.find()) {
            return;
        }
        Configuration configuration = context.getConfiguration();
        InputSplit inputSplit = context.getInputSplit();
        String dirName = ((FileSplit) inputSplit).getPath().getParent().getName();
        reduceKey.set(dirName);
        ExcelConfigSheet excelConfigSheet = excelSheetConfigMap.get(dirName);
        List<OperationMode> operationModeList = excelConfigSheet.getOperationModeList();
        if (operationModeList != null) {
//        if (cutDataMatcher.groupCount() != computingMethodList.size()) {
//            throw new IllegalArgumentException("Regex's group count not equal ");
//        }
            for (OperationMode operationMode : operationModeList) {
                String dataString = cutDataMatcher.group(operationMode.getRegexIndex());
                try {
                    Class<?> klass = configuration.getClass(operationMode.getDataOperationType(), Object.class);

                    Object object = klass.newInstance();
                    if (object instanceof IDataOperator) {
                        Method operationMethod = klass.getMethod("operate", OperationMode.class, String.class);
                        Object returnObject = operationMethod.invoke(object, operationMode, dataString);
                        if (!(Boolean) returnObject) {
                            return;
                        }
                    }
                } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
                    throw new IOException(e);
                }
//                if (operationMode.getType().equals(Date.class)) {
//                    if (!OperationDataForDate.operation(operationMode, dataString)) {
//                        return;
//                    }
//                } else {
//                    if (!OperationDataForString.operation(operationMode, dataString)) {
//                        return;
//                    }
//                }
            }
        }
        context.write(reduceKey, value);
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        PATTERN_CUT_DATA = configuration.getPattern(Constants.ConfigurationKey.REGEX_CUT_DATA, null);

        String operationModeJson = configuration.get(Constants.ConfigurationKey.LIST_OPERATION_MODE);
        excelSheetConfigMap = GSON.fromJson(operationModeJson, new TypeToken<Map<String, ExcelConfigSheet>>() {
        }.getType());
    }
}

