package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.small.download;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-24 下午 3:35.
 */
public class SmDlToHbase extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hdfsInputPath = CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), SmDlToHbase.class.getSimpleName());
            job.setJarByClass(SmDlToHbase.class);

            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.setInputPaths(job, inputPath);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            HTable hTable = new HTable(this.getConf(), "small_download-log");

            FileOutputFormat.setOutputPath(job, outputPath);
            job.setMapperClass(ProbeHbaseMapper.class);
            job.setReducerClass(KeyValueSortReducer.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            job.setPartitionerClass(SimpleTotalOrderPartitioner.class);


            HFileOutputFormat2.configureIncrementalLoad(job, hTable, hTable.getRegionLocator());

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }

            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(this.getConf());
            loader.doBulkLoad(outputPath, hTable);

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    private static class ProbeHbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(line);
            if (!matcher.find()) {
                return;
            }
            long version = System.currentTimeMillis();
            String familyName = "attr";

            String uuid = matcher.group(1);
            String hotel_id = matcher.group(2);
            String room_id = matcher.group(3);
            String timestamps = matcher.group(4);
            String option_type = matcher.group(5);
            String media_type = matcher.group(6);
            String media_id = matcher.group(7);
            String apk_version = matcher.group(8);
            String war_version = matcher.group(9);
            String pro_period = matcher.group(10);
            String ads_period = matcher.group(11);
            String adv_period = matcher.group(12);
            String vod_period = matcher.group(13);
            String custom_volume = matcher.group(14);
            String small_mac = matcher.group(15);
            String time = matcher.group(16);

            Long l = 9999999999999l - Long.valueOf(timestamps);

            byte[] rowKeyBytes = Bytes.toBytes( small_mac + "|" + media_type + "|" + option_type + "|" + l.toString());
            Put put = new Put(rowKeyBytes);// 设置rowkey

            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("uuid"), version, Bytes.toBytes(uuid));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_id"), version, Bytes.toBytes(hotel_id));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("room_id"), version, Bytes.toBytes(room_id));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("timestamps"), version, Bytes.toBytes(timestamps));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("option_type"), version, Bytes.toBytes(option_type));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("media_type"), version, Bytes.toBytes(media_type));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("media_id"), version, Bytes.toBytes(media_id));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("apk_version"), version, Bytes.toBytes(apk_version));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("war_version"), version, Bytes.toBytes(war_version));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("pro_period"), version, Bytes.toBytes(pro_period));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ads_period"), version, Bytes.toBytes(ads_period));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("adv_period"), version, Bytes.toBytes(adv_period));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("vod_period"), version, Bytes.toBytes(vod_period));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("custom_volume"), version, Bytes.toBytes(custom_volume));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("small_mac"), version, Bytes.toBytes(small_mac));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("time"), version, Bytes.toBytes(time));

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
            context.write(rowKey, put);
        }
    }
}
