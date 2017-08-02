/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : demo
 * @Package : net.lizhaoweb.demo.hadoop.mysql2hdfs
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:56
 */
package com.littlehotspot.hadoop.mr.hbase.hotel;

import com.littlehotspot.hadoop.mr.hbase.io.HotelWritable;
import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年08月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class Mysql2HBaseScheduler extends Configured implements Tool {

    //    private String tableName = "savor_media";
    private String hTableName = "hotel";

    @Override
    public int run(String[] args) throws Exception {
        try {
            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);

            // 任务名称
            String jobName = ArgumentFactory.getParameterValue(Argument.JobName);
            System.out.println("\tInput[Job-Name]           : " + jobName);

            // Hdfs 输出路径
            String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);
            System.out.println("\tInput[HDFS-Output-Path]   : " + hdfsOutputPath);
            if (hdfsOutputPath == null) {
                throw new IllegalArgumentException("The argument['" + Argument.OutputPath.getName() + "'] for this program is null");
            }

            String jdbcDriver = null;
            String jdbcUrl = "jdbc:mysql://192.168.2.145:3306/cloud";
            String jdbcUsername = "javaweb";
            String jdbcPassword = "123456";
            String jdbcSql = "SELECT id, name, description, creator, create_time, md5, creator_id, oss_addr, file_path, duration, surfix, type, oss_etag, flag, state, checker_id FROM savor_media WHERE create_time >= '2017-07-01 00:00:00' AND create_time <= '2017-07-30 23:59:59' ORDER BY id ASC";

            // 准备工作
            if (StringUtils.isBlank(jobName)) {
                jobName = this.getClass().getName();
            }
            if (StringUtils.isBlank(jdbcDriver)) {
                jdbcDriver = "com.mysql.jdbc.Driver";
            }

            Path outputPath = new Path(hdfsOutputPath);
            HTable hTable = new HTable(this.getConf(), this.hTableName);

            // 这句话很关键
            this.getConf().set("mapred.job.tracker", "localhost:9001");
            DBConfiguration.configureDB(this.getConf(), jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);

            // 如果输出路径已经存在，则删除
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            Job job = new Job(this.getConf(), jobName);
            job.setJarByClass(this.getClass());

            job.setInputFormatClass(DBInputFormat.class);
            job.setMapperClass(DBInputMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setReducerClass(KeyValueSortReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

            Pattern pattern = Pattern.compile("^.+ (FROM .+) ORDER BY .+$", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(jdbcSql);
            String countSQL = "SELECT COUNT(*) " + matcher.group(1);
            DBInputFormat.setInput(job, HotelWritable.class, jdbcSql, countSQL);

            FileOutputFormat.setOutputPath(job, outputPath);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable, hTable.getRegionLocator());

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }

            // 导入到 HBASE 表中
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(this.getConf());
            loader.doBulkLoad(outputPath, hTable);

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }
}
