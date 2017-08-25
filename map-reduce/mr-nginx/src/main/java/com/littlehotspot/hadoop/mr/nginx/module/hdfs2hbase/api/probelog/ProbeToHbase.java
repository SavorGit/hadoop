package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.probelog;

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
public class ProbeToHbase extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hdfsInputPath = CommonVariables.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), ProbeToHbase.class.getSimpleName());
            job.setJarByClass(ProbeToHbase.class);

            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            HTable hTable = new HTable(this.getConf(), "probe_log");

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

            String probe_mac = matcher.group(1);
            String device_mac = matcher.group(2);
            String signal = matcher.group(3);
            String wifi_type = matcher.group(4);
            String time = matcher.group(5);
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:ss:mm");
            long timestamps = 0;
            try {
                timestamps = format.parse(time).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }

            String hotel_id = matcher.group(6).trim();

            byte[] rowKeyBytes = Bytes.toBytes( probe_mac + "|" + device_mac + "|" + timestamps + "|" + hotel_id);
            Put put = new Put(rowKeyBytes);// 设置rowkey

            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("probe_mac"), version, Bytes.toBytes(probe_mac));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("device_mac"), version, Bytes.toBytes(device_mac));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("signal"), version, Bytes.toBytes(signal));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("wifi_type"), version, Bytes.toBytes(wifi_type));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("time"), version, Bytes.toBytes(timestamps+""));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_id"), version, Bytes.toBytes(hotel_id));

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
            context.write(rowKey, put);
        }
    }
}
