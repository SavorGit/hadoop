package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.boxTimeIndex;

import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
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
import java.util.Iterator;
import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-12 下午 5:58.
 */
public class BoxTimeIndexScheduler extends Configured implements Tool {

    private static final Argument tableSource = new Argument("hBaseTableSource", null, null);

    @Override
    public int run(String[] args) throws Exception {
        try {
            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);

            String tableSourceName = ArgumentFactory.getParameterValue(tableSource);
            String hTableName = ArgumentFactory.getParameterValue(Argument.HbaseTable);

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            Scan scan = new Scan();
            Filter kof = new KeyOnlyFilter();
            scan.setFilter(kof);

            TableMapReduceUtil.initTableMapperJob(tableSourceName, scan, BoxTimeIndexMapper.class, ImmutableBytesWritable.class, Put.class, job, false);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setCombinerClass(BoxTimeIndexCombiner.class);

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

    private static class BoxTimeIndexMapper extends TableMapper<ImmutableBytesWritable, Put> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String row = Bytes.toString(result.getRow());
            Matcher matcher = Constant.ROWKEY_PATTERN.matcher(row);
            if (!matcher.find()) {
                return;
            }
            String mac = matcher.group(1);
            long time = 9999999999999l - Long.parseLong(matcher.group(4));

            byte[] rowKeyBytes = Bytes.toBytes(mac + "|" + time);

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);

            Put put = new Put(rowKeyBytes);// 设置rowkey
            // 基本属性
            String familyName = "attr";
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("box_log-row_key"), Bytes.toBytes(row));

            context.write(rowKey, put);

        }
    }

    private static class BoxTimeIndexCombiner extends Reducer<ImmutableBytesWritable, Put, ImmutableBytesWritable, Put> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {

            String rowString = "";

            String familyName = "attr";
            String column = "box_log-row_key";

            Iterator<Put> textIterator = values.iterator();
            while (textIterator.hasNext()) {
                Put item = textIterator.next();
                if (item == null) {
                    continue;
                }

                Cell row = item.get(Bytes.toBytes(familyName), Bytes.toBytes(column)).get(0);
                rowString += Bytes.toString(row.getValue()) + ",";

            }

            Put put = new Put(key.copyBytes());// 设置rowkey
            // 基本属性
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(rowString));

            context.write(key, put);

        }
    }

}
