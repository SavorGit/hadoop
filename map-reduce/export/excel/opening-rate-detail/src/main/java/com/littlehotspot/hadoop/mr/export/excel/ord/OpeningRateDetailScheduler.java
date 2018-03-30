/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:22
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import com.google.gson.Gson;
import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1>调度器 - 开机率明细</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月30日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class OpeningRateDetailScheduler extends Configured implements Tool {

    private static final Gson GSON = new Gson();

    @Override
    public int run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Jar Clean action configuration");
        System.out.println("=================================================================");

        MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);// 解析参数并初始化 MAP REDUCE
        Map<String, ExcelConfigSheet> excelSheetConfigMap = new ConcurrentHashMap<>();

        // Mapper 输入正则表达式
        String jobName = ArgumentFactory.getParameterValue(Argument.JobName);
        ArgumentFactory.printInputArgument(Argument.JobName, jobName, false);

        // Mapper 输入正则表达式
        String matcherRegex = ArgumentFactory.getParameterValue(Argument.MapperInputFormatRegex);
        ArgumentFactory.printInputArgument(Argument.MapperInputFormatRegex, matcherRegex, false);

        // Hdfs 读取路径
        String[] hdfsInputPathArray = ArgumentFactory.getParameterValues(Argument.InputPath);
        ArgumentFactory.printInputArgument(Argument.InputPath, hdfsInputPathArray);

        // Workbook 路径
        String workbook = ArgumentFactory.getParameterValue(Argument.Workbook);
        ArgumentFactory.printInputArgument(Argument.Workbook, workbook, false);

        // Sheet配置: sheet索引,hdfs目录名,sheet名称,数据中字段的分割符
        String[] sheetArray = ArgumentFactory.getParameterValues(Argument.Sheet);
        ArgumentFactory.printInputArgument(Argument.Sheet, sheetArray);
        Map<String, String> indexHDFSDirMap = this.analysisSheetConfig(excelSheetConfigMap, sheetArray);

        // Title配置: sheetIndex,标题分割符,字段名1|字段名2|...
        String[] titleArray = ArgumentFactory.getParameterValues(Argument.Title);
        ArgumentFactory.printInputArgument(Argument.Title, titleArray);
        this.analysisTitleConfig(excelSheetConfigMap, indexHDFSDirMap, titleArray);

        // OperationMode配置: sheet索引,正则GROUP索引,操作符号,参考值,字段格式,字段类型
        String[] operationModeArray = ArgumentFactory.getParameterValues(Argument.OperationMode);
        ArgumentFactory.printInputArgument(Argument.OperationMode, operationModeArray);
        this.analysisOperationModeConfig(excelSheetConfigMap, indexHDFSDirMap, operationModeArray);

        // 准备工作
        ArgumentFactory.checkNullValueForArgument(Argument.MapperInputFormatRegex, matcherRegex);
        ArgumentFactory.checkNullValuesForArgument(Argument.InputPath, hdfsInputPathArray);
        ArgumentFactory.checkNullValueForArgument(Argument.Workbook, workbook);
        ArgumentFactory.checkNullValuesForArgument(Argument.Sheet, sheetArray);
//        ArgumentFactory.checkNullValueForArgument(Argument.OutputPath, hdfsOutputPath);
        if (StringUtils.isBlank(jobName)) {
            jobName = this.getClass().getName();
        }

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Clean job task now >>>");
        System.out.println();
        System.out.flush();

        this.getConf().setClass(Constants.ConfigurationKey.CLASS_REGISTRAR_PREFIX_DATA_OPERATOR + "string", DataOperatorForString.class, IDataOperator.class);
        this.getConf().setClass(Constants.ConfigurationKey.CLASS_REGISTRAR_PREFIX_DATA_OPERATOR + "date", DataOperatorForDate.class, IDataOperator.class);
        this.getConf().setClass(Constants.ConfigurationKey.CLASS_REGISTRAR_PREFIX_FIELD + "string", String.class, CharSequence.class);
        this.getConf().setClass(Constants.ConfigurationKey.CLASS_REGISTRAR_PREFIX_FIELD + "date", Date.class, Object.class);
        this.getConf().set(Constants.ConfigurationKey.EXCEL_WORKBOOK, workbook);
        this.getConf().setPattern(Constants.ConfigurationKey.REGEX_CUT_DATA, Pattern.compile(matcherRegex));// 配置 Mapper 输入的正则匹配对象
        this.getConf().set(Constants.ConfigurationKey.LIST_OPERATION_MODE, GSON.toJson(excelSheetConfigMap));

        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(this.getClass());


        // 作业输入
        for (String hdfsInputPath : hdfsInputPathArray) {
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);
        }
        job.setMapperClass(OpeningRateDetailMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(OpeningRateDetailReducer.class);
        job.setNumReduceTasks(1);

        // 作业输出
        Path outputPath = new Path("/tmp/.john/export/excel/");
        FileOutputFormat.setOutputPath(job, outputPath);

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
        fileSystem.delete(outputPath, true);
        return 0;
    }

    private void analysisOperationModeConfig(Map<String, ExcelConfigSheet> excelSheetConfigMap, Map<String, String> indexHDFSDirMap, String[] operationModeArray) {
        Pattern operationModeConfigPattern = Pattern.compile("^(\\d+),(\\d+),([=><!]+),([^,]+),([^,]+),([^,]+),([^,]+)$");
        for (String operationModeString : operationModeArray) {
            Matcher matcher = operationModeConfigPattern.matcher(operationModeString);
            if (matcher.find()) {
                String sheetIndexString = matcher.group(1);
                String regexIndexString = matcher.group(2);
                String operationSymbolString = matcher.group(3);
                String value = matcher.group(4);
                String format = matcher.group(5);
                String dataOperationType = matcher.group(6);
                String fieldType = matcher.group(7);

                String hdfsDir = indexHDFSDirMap.get(sheetIndexString);
                ExcelConfigSheet excelConfigSheet = excelSheetConfigMap.get(hdfsDir);
                if (excelConfigSheet == null) {
                    continue;
                }

                List<OperationMode> operationModeList = excelConfigSheet.getOperationModeList();
                if (operationModeList == null) {
                    operationModeList = new ArrayList<>();
                    excelConfigSheet.setOperationModeList(operationModeList);
                }
                int sheetIndex = Integer.valueOf(sheetIndexString);
                int regexIndex = Integer.valueOf(regexIndexString);

                OperationSymbol operationSymbol = OperationSymbol.fromName(operationSymbolString);
                if (operationSymbol == null) {
                    throw new IllegalStateException(String.format("Not found operation-symbol '%s'", operationSymbolString));
                }

                Class<?> dataOperationClass = ClassRegistrar.dataOperatorClass(dataOperationType);
                if (dataOperationClass == null) {
                    throw new IllegalStateException(String.format("Not found data-operator-class '%s'", dataOperationType));
                }

                Class<?> fieldClass = ClassRegistrar.fieldClass(fieldType);
                if (fieldClass == null) {
                    throw new IllegalStateException(String.format("Not found data-type-class '%s'", fieldType));
                }

                OperationMode operationMode = new OperationMode(
                        sheetIndex,
                        regexIndex,
                        operationSymbol,
                        value,
                        format,
                        Constants.ConfigurationKey.CLASS_REGISTRAR_PREFIX_DATA_OPERATOR + dataOperationType,
                        Constants.ConfigurationKey.CLASS_REGISTRAR_PREFIX_FIELD + fieldType
                );

                operationModeList.add(operationMode);
            }
        }
    }

    private void analysisTitleConfig(Map<String, ExcelConfigSheet> excelSheetConfigMap, Map<String, String> indexHDFSDirMap, String[] titleArray) {
        Pattern sheetConfigPattern = Pattern.compile("^(\\d+),([^,]+),([^,]+)$");
        for (String titleConfig : titleArray) {
            Matcher matcher = sheetConfigPattern.matcher(titleConfig);
            if (matcher.find()) {
                String sheetIndexString = matcher.group(1);
                String titleRegexSeparator = matcher.group(2);
                String titles = matcher.group(3);

                String hdfsDir = indexHDFSDirMap.get(sheetIndexString);
                ExcelConfigSheet excelConfigSheet = excelSheetConfigMap.get(hdfsDir);
                if (excelConfigSheet == null) {
                    continue;
                }
                excelConfigSheet.setTitles(titles);
                excelConfigSheet.setTitleRegexSeparator(titleRegexSeparator);
            }
        }
    }

    private Map<String, String> analysisSheetConfig(Map<String, ExcelConfigSheet> excelSheetConfigMap, String[] sheetArray) {
        Map<String, String> indexHDFSDirMap = new ConcurrentHashMap<>();
        Pattern sheetConfigPattern = Pattern.compile("^(\\d+),(\\w+),([^,]+),([^,]+)$");
        for (String sheetConfig : sheetArray) {
            Matcher matcher = sheetConfigPattern.matcher(sheetConfig);
            if (matcher.find()) {
                String indexString = matcher.group(1);
                String hdfsDir = matcher.group(2);
                String name = matcher.group(3);
                String fieldRegexSeparator = matcher.group(4);
                ExcelConfigSheet excelConfigSheet = excelSheetConfigMap.get(hdfsDir);
                if (excelConfigSheet == null) {
                    excelConfigSheet = new ExcelConfigSheet();
                }
                int index = Integer.valueOf(indexString);
                excelConfigSheet.setIndex(index);
                excelConfigSheet.setName(name);
                excelConfigSheet.setHdfsDir(hdfsDir);
                excelConfigSheet.setFieldRegexSeparator(fieldRegexSeparator);

                indexHDFSDirMap.put(indexString, hdfsDir);
                excelSheetConfigMap.put(hdfsDir, excelConfigSheet);
            }
        }
        return indexHDFSDirMap;
    }
}
