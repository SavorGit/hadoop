/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.hive
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 18:43
 */
package com.littlehotspot.hadoop.mr.export.excel.hive;

import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.HiveInputFormat;
import org.apache.hadoop.mapreduce.lib.db.SimpleDataWritable;
import org.apache.hadoop.mapreduce.lib.excel.ExcelFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.sql.SQLSyntaxErrorException;
import java.util.regex.Pattern;

/**
 * <h1>调度器 - Hive 导出 Excel</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月14日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ExportExcelFromJDBCScheduler extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Jar action configuration");
        System.out.println("=================================================================");

        MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);// 解析参数并初始化 MAP REDUCE

        // Mapper 输入正则表达式
        String jobName = ArgumentFactory.getParameterValue(Argument.JobName);
        ArgumentFactory.printInputArgument(Argument.JobName, jobName, false);

        // JDBC 驱动器
        String jdbcDriver = ArgumentFactory.getParameterValue(Argument.JDBCDriver);
        ArgumentFactory.printInputArgument(Argument.JDBCDriver, jdbcDriver, false);

        // JDBC 连接字符串
        String jdbcUrl = ArgumentFactory.getParameterValue(Argument.JDBCUrl);
        ArgumentFactory.printInputArgument(Argument.JDBCUrl, jdbcUrl, false);

        // JDBC 用户
        String jdbcUser = ArgumentFactory.getParameterValue(Argument.JDBCUsername);
        ArgumentFactory.printInputArgument(Argument.JDBCUsername, jdbcUser, false);

        // JDBC 密码
        String jdbcPass = ArgumentFactory.getParameterValue(Argument.JDBCPassword);
        ArgumentFactory.printInputArgument(Argument.JDBCPassword, jdbcPass, true);

        // Workbook 路径
        String workbook = ArgumentFactory.getParameterValue(Argument.Workbook);
        ArgumentFactory.printInputArgument(Argument.Workbook, workbook, false);

        // Sheet配置: sheet索引,hdfs目录名,sheet名称,数据中字段的分割符
        String sheetName = ArgumentFactory.getParameterValue(Argument.Sheet);
        ArgumentFactory.printInputArgument(Argument.Sheet, sheetName, false);

        // Title配置: sheetIndex,标题分割符,字段名1|字段名2|...
        String sheetTitles = ArgumentFactory.getParameterValue(Argument.Title);
        ArgumentFactory.printInputArgument(Argument.Title, sheetTitles, false);

        // 要执行的 SQL 语句
        String executeSQL = ArgumentFactory.getParameterValue(Argument.JDBCSql);
        ArgumentFactory.printInputArgument(Argument.JDBCSql, executeSQL, false);

        // 输入 Reduce 时，Value 的正则表达式
        String reduceInPatternValue = ArgumentFactory.getParameterValue(ExcelArgument.ReduceInPatternValue);
        ArgumentFactory.printInputArgument(ExcelArgument.ReduceInPatternValue, reduceInPatternValue, false);


        // 准备工作
        ArgumentFactory.checkNullValueForArgument(Argument.JDBCDriver, jdbcUrl);
        ArgumentFactory.checkNullValueForArgument(Argument.JDBCUrl, jdbcUrl);
        ArgumentFactory.checkNullValueForArgument(Argument.Workbook, workbook);
        ArgumentFactory.checkNullValueForArgument(Argument.Sheet, sheetName);
        ArgumentFactory.checkNullValueForArgument(Argument.JDBCSql, executeSQL);
        ArgumentFactory.checkNullValueForArgument(ExcelArgument.ReduceInPatternValue, reduceInPatternValue);
        if (StringUtils.isBlank(jobName)) {
            jobName = this.getClass().getName();
        }

        String[] sheetTitleArray = null;
        if (sheetTitles != null) {
            sheetTitleArray = sheetTitles.split("\\|");
        }

        Pattern valuePattern = Pattern.compile(reduceInPatternValue);

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Export job task now >>>");
        System.out.println();
        System.out.flush();

        Path outputPath = new Path(workbook);

        DBConfiguration.configureDB(this.getConf(), jdbcDriver, jdbcUrl, jdbcUser, jdbcPass);

        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(this.getClass());

        job.setMapperClass(HiveInputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(ExcelFileOutputFormat.class);

        // 设置输入输出
        HiveInputFormat.setInput(job, SimpleDataWritable.class, executeSQL, this.getCountQuery(executeSQL));
        ExcelFileOutputFormat.setOutputPath(job, outputPath, sheetName, sheetTitleArray, valuePattern);

        boolean status = job.waitForCompletion(true);
        if (!status) {
            throw new Exception("MapReduce task execute failed.........");
        }

        return 0;
    }

    // 生成获取总记录数据的 SQL 语句
    private String getCountQuery(String selectQuery) {
        StringBuilder query = new StringBuilder("SELECT COUNT(*)");
        String selectQueryToUpperCase = selectQuery.toUpperCase();
        int firstFromIndex = selectQueryToUpperCase.indexOf(" FROM ");
        if (firstFromIndex < 0) {
            throw new RuntimeException(new SQLSyntaxErrorException("Not found from clause in SQL '" + selectQuery + "'"));
        }
        int lastOrderIndex = selectQueryToUpperCase.lastIndexOf(" ORDER BY ");
        if (lastOrderIndex < 0) {
            query.append(selectQuery.substring(firstFromIndex));
        } else {
            query.append(selectQuery.substring(firstFromIndex, lastOrderIndex));
        }
        return query.toString();
    }

    public static class HiveInputMapper extends Mapper<LongWritable, SimpleDataWritable, Text, Text> {
        @Override
        public void map(LongWritable key, SimpleDataWritable value, Context context) throws IOException, InterruptedException {
//            System.out.println(key + "\t" + value);
            context.write(new Text(key.toString()), new Text(value.toString()));
        }
    }
}
