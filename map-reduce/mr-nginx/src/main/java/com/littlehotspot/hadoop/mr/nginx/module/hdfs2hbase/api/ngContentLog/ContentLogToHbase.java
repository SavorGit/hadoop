package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog;

import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import org.apache.hadoop.conf.Configuration;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-04 下午 10:06.
 */
public class ContentLogToHbase extends Configured implements Tool {
    private static final Argument regex = new Argument("regex", null, null);

    @Override
    public int run(String[] args) throws Exception {
        try {

            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);

            // 任务名称
            String[] regexes = ArgumentFactory.getParameterValues(regex);
            StringBuffer regex = new StringBuffer();
            for (String s : regexes) {
                regex.append(s).append("|");
            }
            this.getConf().set("regex", regex.toString());

            // 获取参数
            String hdfsInputPath = ArgumentFactory.getParameterValue(Argument.InputPath);
            String hdfsOutputPath = ArgumentFactory.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            /**作业输入*/
            Path inputPath = new Path(hdfsInputPath);
            FileInputFormat.addInputPath(job, inputPath);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            HTable hTable = new HTable(this.getConf(), "ng_content_log");

            FileOutputFormat.setOutputPath(job, outputPath);
            job.setMapperClass(ContentLogMapper.class);
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

    private static class ContentLogMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private String[] regexes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String regex = configuration.get("regex");

            this.regexes = regex.split("\\|");

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = CommonVariables.MAPPER_HDFS_FORMAT_REGEX.matcher(line);
            if (!matcher.find()) {
                return;
            }

            String requestUrl = matcher.group(4);
            Matcher matcherRequest = null;
            boolean find = false;
            for (String regex : this.regexes) {
                matcherRequest = Pattern.compile(regex).matcher(requestUrl);
                if (matcherRequest.find()) {
                    requestUrl = matcherRequest.group().split("(\\\\x.*)?\\s")[1];
                    find = true;
                }
            }

            if (!find) {
                return;
            }

            // 从url中获取参数
            String contentId = "0";
            String channel = "";
            String isSq = "0";
            if (requestUrl.matches("(.*)\\/content\\/(\\d{1,})\\.html(.*)")) {
                contentId = matcherRequest.group(1);
                String params = matcherRequest.group(2);
                String[] split = params.split("&");
                for (String s : split) {
                    if (s.contains("channel=")) {
                        channel = s.substring(s.lastIndexOf("=") + 1);
                    }
                    if (s.contains("issq=")) {
                        isSq = s.substring(s.lastIndexOf("=") + 1);
                    }
                }
            }

            // 从日直记录中获取展示的参数
            String ip;
            String isWx = "0";
            String netType = "";
            String deviceType = "";
            long timestamp = 0;

            ip = matcher.group(1);
            try {
                String s = matcher.group(3).substring(0, 19);
                timestamp = CommonVariables.dateFormat.parse(s).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }

            String httpUserAgent = matcher.group(9);
            Matcher matcher1 = CommonVariables.MAPPER_WX.matcher(httpUserAgent);
            if (matcher1.find()) {
                isWx = "1";
            }
            Matcher matcher2 = CommonVariables.MAPPER_NET_TYPE.matcher(httpUserAgent);
            if (matcher2.find()) {
                netType = matcher2.group(1);
            }

            Matcher matcher4 = CommonVariables.MAPPER_DEVICE_iPhone.matcher(httpUserAgent);
            if (matcher4.find()) {
                deviceType = "ios";
            }
            Matcher matcher5 = CommonVariables.MAPPER_DEVICE_Android.matcher(httpUserAgent);
            if (matcher5.find()) {
                deviceType = "android";
            }
            Matcher matcher6 = CommonVariables.MAPPER_DEVICE_MAC.matcher(httpUserAgent);
            if (matcher6.find()) {
                deviceType = "mac";
            }
            Matcher matcher7 = CommonVariables.MAPPER_DEVICE_WINDOWS.matcher(httpUserAgent);
            if (matcher7.find()) {
                deviceType = "windows";
            }//


            String familyName = "attr";
            long version = System.currentTimeMillis();

            byte[] rowKeyBytes = Bytes.toBytes(ip + "|" + timestamp + "|" + deviceType + "|" + contentId);
            Put put = new Put(rowKeyBytes);// 设置rowkey

            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ip"), version, Bytes.toBytes(ip));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("is_wx"), version, Bytes.toBytes(isWx));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("net_type"), version, Bytes.toBytes(netType));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("device_type"), version, Bytes.toBytes(deviceType));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("timestamp"), version, Bytes.toBytes(timestamp + ""));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("content_id"), version, Bytes.toBytes(contentId));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("channel"), version, Bytes.toBytes(channel));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("is_sq"), version, Bytes.toBytes(isSq));
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("request_url"), version, Bytes.toBytes(requestUrl));

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
            context.write(rowKey, put);

        }
    }
}
