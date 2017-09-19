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
package com.littlehotspot.hadoop.mr.hbase;

import com.littlehotspot.hadoop.mr.hbase.area.AreaWritable;
import com.littlehotspot.hadoop.mr.hbase.area.DBInputAreaMapper;
import com.littlehotspot.hadoop.mr.hbase.box.BoxWritable;
import com.littlehotspot.hadoop.mr.hbase.box.DBInputBoxMapper;
import com.littlehotspot.hadoop.mr.hbase.category.CategoryWritable;
import com.littlehotspot.hadoop.mr.hbase.category.DBInputCategoryMapper;
import com.littlehotspot.hadoop.mr.hbase.hotel.DBInputHotelMapper;
import com.littlehotspot.hadoop.mr.hbase.hotel.HotelWritable;
import com.littlehotspot.hadoop.mr.hbase.hotelBoxIndex.DBInputHotelBoxIndexMapper;
import com.littlehotspot.hadoop.mr.hbase.hotelBoxIndex.HotelBoxIndexWritable;
import com.littlehotspot.hadoop.mr.hbase.medias.DBInputMediasMapper;
import com.littlehotspot.hadoop.mr.hbase.medias.MediasWritable;
import com.littlehotspot.hadoop.mr.hbase.resources.DBInputResourcesMapper;
import com.littlehotspot.hadoop.mr.hbase.resources.ResourcesWritable;
import com.littlehotspot.hadoop.mr.hbase.room.DBInputRoomMapper;
import com.littlehotspot.hadoop.mr.hbase.room.RoomWritable;
import com.littlehotspot.hadoop.mr.hbase.tv.DBInputTvMapper;
import com.littlehotspot.hadoop.mr.hbase.tv.TvWritable;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
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

    @Override
    public int run(String[] args) throws Exception {
        try {
            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);

            String jobName = ArgumentFactory.getParameterValue(Argument.JobName);
            ArgumentFactory.printInputArgument(Argument.JobName, jobName, false);

            String jdbcDriver = ArgumentFactory.getParameterValue(Argument.JDBCDriver);
            ArgumentFactory.printInputArgument(Argument.JDBCDriver, jdbcDriver, false);

            String jdbcUrl = ArgumentFactory.getParameterValue(Argument.JDBCUrl);
            ArgumentFactory.printInputArgument(Argument.JDBCUrl, jdbcUrl, false);

            String jdbcUsername = ArgumentFactory.getParameterValue(Argument.JDBCUsername);
            ArgumentFactory.printInputArgument(Argument.JDBCUsername, jdbcUsername, false);

            String jdbcPassword = ArgumentFactory.getParameterValue(Argument.JDBCPassword);
            ArgumentFactory.printInputArgument(Argument.JDBCPassword, jdbcPassword, true);

            String jdbcSql = ArgumentFactory.getParameterValue(Argument.JDBCSql);
            ArgumentFactory.printInputArgument(Argument.JDBCSql, jdbcSql, false);

            String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);
            ArgumentFactory.printInputArgument(Argument.OutputPath, hdfsOutputPath, false);

            String hTableName = ArgumentFactory.getParameterValue(Argument.HbaseTable);
            ArgumentFactory.printInputArgument(Argument.HbaseTable, hTableName, false);

            // 准备工作
            ArgumentFactory.checkNullValueForArgument(Argument.JDBCUrl, jdbcUrl);
            ArgumentFactory.checkNullValueForArgument(Argument.JDBCSql, jdbcSql);
            ArgumentFactory.checkNullValueForArgument(Argument.OutputPath, hdfsOutputPath);
            ArgumentFactory.checkNullValueForArgument(Argument.HbaseTable, hTableName);
            if (StringUtils.isBlank(jobName)) {
                jobName = this.getClass().getName();
            }
            if (StringUtils.isBlank(jdbcDriver)) {
                jdbcDriver = "com.mysql.jdbc.Driver";
            }
            Class<?> writableClass = null;
            Class<?> mapperClass = null;
            if (hTableName.equals("hotel")) {
                writableClass = HotelWritable.class;
                mapperClass = DBInputHotelMapper.class;
            } else if (hTableName.equals("room")) {
                writableClass = RoomWritable.class;
                mapperClass = DBInputRoomMapper.class;
            } else if (hTableName.equals("box")) {
                writableClass = BoxWritable.class;
                mapperClass = DBInputBoxMapper.class;
            } else if (hTableName.equals("hotel_box_index")) {
                writableClass = HotelBoxIndexWritable.class;
                mapperClass = DBInputHotelBoxIndexMapper.class;
            } else if (hTableName.equals("medias")) {
                writableClass = MediasWritable.class;
                mapperClass = DBInputMediasMapper.class;
            } else if (hTableName.equals("resources")) {
                writableClass = ResourcesWritable.class;
                mapperClass = DBInputResourcesMapper.class;
            } else if (hTableName.equals("tv")) {
                writableClass = TvWritable.class;
                mapperClass = DBInputTvMapper.class;
            } else if (hTableName.equals("area")) {
                writableClass = AreaWritable.class;
                mapperClass = DBInputAreaMapper.class;
            } else if (hTableName.equals("category")) {
                writableClass = CategoryWritable.class;
                mapperClass = DBInputCategoryMapper.class;
            }

            Path outputPath = new Path(hdfsOutputPath);

            // 这句话很关键
//            this.getConf().set("mapred.job.tracker", "localhost:9001");
            DBConfiguration.configureDB(this.getConf(), jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);

            // 如果输出路径已经存在，则删除
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            Job job = Job.getInstance(this.getConf(), jobName);
            job.setJarByClass(this.getClass());

            job.setInputFormatClass(DBInputFormat.class);
            job.setMapperClass((Class<? extends Mapper>) mapperClass);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setReducerClass(KeyValueSortReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

            String countSQL = this.getCountSql(jdbcSql);
            DBInputFormat.setInput(job, (Class<? extends DBWritable>) writableClass, jdbcSql, countSQL);

            FileOutputFormat.setOutputPath(job, outputPath);

            HTable hTable = new HTable(this.getConf(), hTableName);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable, hTable.getRegionLocator());

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }

            // 导入到 HBASE 表中
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(this.getConf());
            loader.doBulkLoad(outputPath, hTable);

            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    private String getCountSql(String jdbcSql) {
        Pattern pattern = Pattern.compile("^SELECT\\s+[a-z0-9_ ,.\\s]+\\s+(FROM\\s+.+)$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(jdbcSql);
        if (!matcher.find()) {
            throw new RuntimeException("SQL statement error");
        }
        String countSQL = matcher.group(1);
        pattern = Pattern.compile("^(FROM\\s+.+)\\s+ORDER\\s+BY\\s+.+$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(countSQL);
        if (matcher.find()) {
            countSQL = matcher.group(1);
        }
        countSQL = String.format("SELECT COUNT(*) %s", countSQL);
        return countSQL;
    }
}
