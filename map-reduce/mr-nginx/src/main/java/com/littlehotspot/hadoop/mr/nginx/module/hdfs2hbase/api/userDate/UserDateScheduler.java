/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:31
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.userDate;

import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <h1>调度器 - 用户按天统计 [API]</h1>
 *
 */
public class UserDateScheduler extends Configured implements Tool {

    private static final Argument dateArg = new Argument("date", null, null);

    private static final Argument aType = new Argument("actType", null, null);

    private static final Argument tableSource = new Argument("hBaseTableSource", null, null);

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    @Override
    public int run(String[] args) throws Exception {
        try {
            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);

            String date = ArgumentFactory.getParameterValue(dateArg);
            this.getConf().set("date", date);
            long startTime = dateFormat.parse(date).getTime();
            long endTime = startTime + 1 * 24 * 60 * 60 * 1000;

            String tableSourceName = ArgumentFactory.getParameterValue(tableSource);
            String hTableName = ArgumentFactory.getParameterValue(Argument.HbaseTable);

            String actType = ArgumentFactory.getParameterValue(aType);
            this.getConf().set("actType", actType);

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("device_id"));
            scan.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("p_time"));
            scan.addColumn(Bytes.toBytes("attr"), Bytes.toBytes("v_time"));

            //设置过滤器
            List<Filter> filters = new ArrayList<Filter>();

            BinaryComparator comp = new BinaryComparator(Bytes.toBytes(startTime + ""));
            SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("start"), CompareFilter.CompareOp.GREATER_OR_EQUAL, comp);

            BinaryComparator comp1 = new BinaryComparator(Bytes.toBytes(endTime + ""));
            SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("start"), CompareFilter.CompareOp.LESS, comp1);
            filters.add(startFilter);
            filters.add(endFilter);

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);

            TableMapReduceUtil.initTableMapperJob(tableSourceName, scan, DateMapper.class, ImmutableBytesWritable.class, Put.class, job, false);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setCombinerClass(DateCombiner.class);

            job.setReducerClass(KeyValueSortReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            HTable hTable = new HTable(this.getConf(), hTableName);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable, hTable.getRegionLocator());

            // 执行任务
            boolean state = job.waitForCompletion(true);
            if (!state) {
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

    public static class DateMapper extends TableMapper<ImmutableBytesWritable, Put> {

        static String date;
        static ActType actType;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            date = configuration.get("date");
            actType = ActType.valueOf(configuration.get("actType"));
        }

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {

            try {
                String deviceId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("device_id")));

                if (deviceId == null) {
                    return;
                }
                byte[] rowKeyBytes = Bytes.toBytes(deviceId + "|" + date);

                ImmutableBytesWritable rowKey1 = new ImmutableBytesWritable(rowKeyBytes);
                UserDate userByDate = new UserDate(result, date, actType);

                context.write(rowKey1, userByDate.toPut(actType));
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

    private static class DateCombiner extends Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, Put> {
        static ActType actType;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            actType = ActType.valueOf(context.getConfiguration().get("actType"));
        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

            long dema_count = 0;
            long dema_time = 0;
            long proje_count = 0;
            long proje_time = 0;
            long read_count = 0;
            long read_time = 0;

            String family = "attr";

            Iterator<Put> textIterator = values.iterator();
            UserDate userByDate = new UserDate(key);
            switch (actType) {
                case DEMAND:
                    while (textIterator.hasNext()) {
                        Put item = textIterator.next();
                        if (item == null) {
                            continue;
                        }

                        Cell d_time = item.get(Bytes.toBytes(family), Bytes.toBytes("dema_time")).get(0);
                        dema_time += Long.parseLong(Bytes.toString(d_time.getValue()));
                        dema_count++;
                    }

                    userByDate.setDema_count(dema_count);
                    userByDate.setDema_time(dema_time);
                    break;
                case PROJECTION:
                    while (textIterator.hasNext()) {
                        Put item = textIterator.next();
                        if (item == null) {
                            continue;
                        }

                        Cell p_time = item.get(Bytes.toBytes(family), Bytes.toBytes("proje_time")).get(0);
                        proje_time += Long.parseLong(Bytes.toString(p_time.getValue()));
                        proje_count++;
                    }

                    userByDate.setProje_count(proje_count);
                    userByDate.setProje_time(proje_time);
                    break;
                case READ:
                    while (textIterator.hasNext()) {
                        Put item = textIterator.next();
                        if (item == null) {
                            continue;
                        }

                        Cell r_time = item.get(Bytes.toBytes(family), Bytes.toBytes("read_time")).get(0);
                        read_time += Long.parseLong(Bytes.toString(r_time.getValue()));
                        read_count++;
                    }

                    userByDate.setRead_count(read_count);
                    userByDate.setRead_time(read_time);
                    break;
            }

            context.write(key, userByDate.toPut(actType));
        }

    }

}
