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
import org.apache.hadoop.fs.FileSystem;
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
//        Map<String, ExcelConfigSheet> excelSheetConfigMap = new ConcurrentHashMap<>();

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
        ArgumentFactory.printInputArgument(Argument.JDBCPassword, jdbcPass, false);

        // Workbook 路径
        String workbook = ArgumentFactory.getParameterValue(Argument.Workbook);
        ArgumentFactory.printInputArgument(Argument.Workbook, workbook, false);

        // Sheet配置: sheet索引,hdfs目录名,sheet名称,数据中字段的分割符
        String[] sheetArray = ArgumentFactory.getParameterValues(Argument.Sheet);
        ArgumentFactory.printInputArgument(Argument.Sheet, sheetArray);
//        Map<String, String> indexHDFSDirMap = this.analysisSheetConfig(excelSheetConfigMap, sheetArray);

        // Title配置: sheetIndex,标题分割符,字段名1|字段名2|...
        String[] titleArray = ArgumentFactory.getParameterValues(Argument.Title);
        ArgumentFactory.printInputArgument(Argument.Title, titleArray);
//        this.analysisTitleConfig(excelSheetConfigMap, indexHDFSDirMap, titleArray);
        // Title配置: sheetIndex,标题分割符,字段名1|字段名2|...


        // 准备工作
//        ArgumentFactory.checkNullValueForArgument(Argument.MapperInputFormatRegex, matcherRegex);
//        ArgumentFactory.checkNullValuesForArgument(Argument.InputPath, hdfsInputPathArray);
//        ArgumentFactory.checkNullValueForArgument(Argument.Workbook, workbook);
//        ArgumentFactory.checkNullValuesForArgument(Argument.Sheet, sheetArray);
        if (StringUtils.isBlank(jobName)) {
            jobName = this.getClass().getName();
        }

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Export job task now >>>");
        System.out.println();
        System.out.flush();

        Path outputPath = new Path(workbook);

        DBConfiguration.configureDB(this.getConf(), jdbcDriver, jdbcUrl, jdbcUser, jdbcPass);

        // 如果输出路径已经存在，则删除
        FileSystem fileSystem = FileSystem.newInstance(this.getConf());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        this.getConf().setPattern(ExcelFileOutputFormat.DATA_PATTERN, Pattern.compile("^(.+)\\u0001(.+)$"));

        Job job = Job.getInstance(this.getConf(), jobName);
        job.setJarByClass(this.getClass());

        job.setMapperClass(HiveInputMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(ExcelFileOutputFormat.class);


        String mediaSelectQuery = "SELECT id, name FROM mysql.savor_media WHERE create_time >= '2017-07-01 00:00:00' AND create_time <= '2017-07-30 23:59:59' ORDER BY id ASC";
//        String mediaSelectQuery = "SELECT id, name, description, creator, create_time, md5, creator_id, oss_addr, file_path, duration, surfix, type, oss_etag, flag, state, checker_id FROM mysql.savor_media WHERE create_time >= '2017-07-01 00:00:00' AND create_time <= '2017-07-30 23:59:59' ORDER BY id ASC";
        HiveInputFormat.setInput(job, SimpleDataWritable.class, mediaSelectQuery, this.getCountQuery(mediaSelectQuery));
//        String hotelSelectQuery = "SELECT * FROM mysql.savor_hotel";
//        HiveInputFormat.setInput(job, SimpleDataWritable.class, hotelSelectQuery, this.getCountQuery(hotelSelectQuery));
        ExcelFileOutputFormat.setOutputPath(job, outputPath);

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
            System.out.println(key + "\t" + value);
            context.write(new Text(value.toString()), new Text());
        }
    }
}
