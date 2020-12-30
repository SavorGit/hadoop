/*
 * Copyright (c) 2020, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.netty.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:43
 */
package com.littlehotspot.hadoop.mr.netty.box;

import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.oozie.action.hadoop.LauncherMain;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 机顶盒连接Netty数据转换
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2020年12月25日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@SuppressWarnings({"AccessStaticViaInstance", "WeakerAccess", "JavaDoc", "UnusedAssignment", "unused"})
public class ConvertFatalData extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Jar netty-push fatal-log action configuration");
        System.out.println("=================================================================");


        // 定义参数
        Options options = new Options();
        options.addOption("h", "help", false, "Print options' information");
        options.addOption("jn", "jobName", true, "The name of job");
        options.addOption("o", "output", true, "The path of data output");

        // withValueSeparator(char sep)指定参数值之间的分隔符
        Option inputOption = OptionBuilder.withArgName("args")
                .withLongOpt("input")
                .hasArgs()
                .withValueSeparator(',')
                .withDescription("The paths of data input")
                .create("i");
        options.addOption(inputOption);

        Option verboseOption = OptionBuilder.withLongOpt("verbose")
                .withDescription("Print the progress to the user")
                .create();
        options.addOption(verboseOption);


//        Option property = OptionBuilder.withArgName("property=name")
//                .hasArgs()
//                .withValueSeparator()
//                .withDescription("use value for a property")
//                .create("D");
//        options.addOption(property);

        boolean verbose = false;
        String _jobName = null;
        String[] _inputPaths = null;
        String _outputPath = null;


        // 解析参数
        CommandLineParser parser = new GnuParser();
//        try {
        CommandLine cli = parser.parse(options, args);
        if (cli.hasOption('h')) {
            HelpFormatter hf = new HelpFormatter();
            hf.printHelp("Options", options);
            return 0;
        } else {
            _jobName = cli.getOptionValue("jn");
            System.out.println("\t[Input]Job name    : " + _jobName);
            _inputPaths = cli.getOptionValues("i");
            System.out.println("\t[Input]Input paths : " + Arrays.asList(_inputPaths));
            _outputPath = cli.getOptionValue("o");
            System.out.println("\t[Input]Output path : " + _outputPath);
            verbose = cli.hasOption("verbose");
            System.out.println("\t[Input]Verbose     : " + verbose);
//
//            Properties properties = cli.getOptionProperties("D");
//            String ext = properties.getProperty("ext");
//            String dir = properties.getProperty("dir");
//            System.out.println("property ext: " + ext + "\tdir:" + dir);
        }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        // 校验参数
        if (StringUtils.isBlank(_jobName)) {
            _jobName = this.getClass().getName();
        }
        if (_inputPaths.length < 1) {
            throw new Exception("The paths of data input is not set.");
        }
        if (StringUtils.isBlank(_outputPath)) {
            throw new Exception("The paths of data output is not set.");
        }
        System.out.println("\n\tJob name    : " + _jobName);
        System.out.println("\tInput paths : " + Arrays.asList(_inputPaths));
        System.out.println("\tOutput path : " + _outputPath);


        System.out.println("=================================================================");
        System.out.println("\n>>> Invoking netty-push fatal-log job task now >>>\n");
        System.out.flush();

        Job job = Job.getInstance(this.getConf(), _jobName);
        job.setJarByClass(this.getClass());


        // 作业输入
        for (String _inputPath : _inputPaths) {
            if (StringUtils.isBlank(_inputPath)) {
                continue;
            }
            Path inputPath = new Path(_inputPath);
            FileInputFormat.addInputPath(job, inputPath);// 如果此处不设置的话，可以通过mapreduce.input.fileinputformat.inputdir来设置。
        }
        job.setMapperClass(_Mapper.class);// 如果此处不设置的话，可以通过mapreduce.job.map.class来设置。
        job.setMapOutputKeyClass(Text.class);// 如果此处不设置的话，可以通过mapreduce.map.output.key.class来设置。
        job.setMapOutputValueClass(Text.class);// 如果此处不设置的话，可以通过mapreduce.map.output.value.class来设置。
//        job.setInputFormatClass(CombineTextInputFormat.class);// 如果此处不设置的话，可以通过mapreduce.job.inputformat.class来设置，配合mapreduce.input.fileinputformat.split.maxsize使用。

//        Path[] inputPaths = new Path[_inputPaths.length];
//        for (int index = 0; index < _inputPaths.length; index++) {
//            inputPaths[index] = new Path(_inputPaths[index]);
//        }
//        FileInputFormat.setInputPaths(job, inputPaths);// 如果此处不设置的话，可以通过mapreduce.input.fileinputformat.inputdir来设置。
//        job.setMapperClass(StartUpFeeMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);

//        job.setCombinerClass(StartUpFeeReducer.class);


        // 作业输出
        Path outputPath = new Path(_outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);// 如果此处不设置的话，可以通过mapreduce.output.fileoutputformat.outputdir来设置。
        job.setReducerClass(_Reducer.class);// 如果此处不设置的话，可以通过mapreduce.job.reduce.class来设置。
        job.setOutputKeyClass(Text.class);// 如果此处不设置的话，可以通过mapreduce.job.output.key.class来设置。
        job.setOutputValueClass(NullWritable.class);// 如果此处不设置的话，可以通过mapreduce.job.output.value.class来设置。
//        job.setOutputFormatClass(TextOutputFormat.class);// 如果此处不设置的话，可以通过mapreduce.job.outputformat.class来设置。

//        job.setJar("E:\\WorkSpace\\Company\\Savor\\Git\\JAVA\\hadoop\\map-reduce\\box-statistics\\start-up\\target\\start-up-LHS.HADOOP.2.11.1.0.1.0.0-SNAPSHOT.jar");
        boolean status = job.waitForCompletion(verbose);
        if (!status) {
            String exceptionMessage = String.format("MapReduce[Netty-Fatal] task[%s] execute failed", _jobName);
            throw new Exception(exceptionMessage);
        }
        return 0;
    }

    /**
     * LongWritable 偏移量 long，表示该行在文件中的位置，而不是行号
     * Text map阶段的输入数据 一行文本信息 字符串类型 String
     * Text map阶段的数据字符串类型 String
     * IntWritable map阶段输出的value类型，对应java中的int型，表示行号
     */
    public static class _Mapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Pattern MULTIPLE_CHANNEL = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}) ERROR FATAL \\[nioEventLoopGroup-\\d+-\\d+\\] [a-zA-Z0-9)(:_.]+ -Multiple channel\\(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+\\)\\[[nul0-9.]+/([a-zA-Z0-9]+) : ([a-zA-Z0-9]+)\\] is registered, SerialNO\\[([a-zA-Z0-9-_]+)\\]$");
        private static final Pattern SEND_HEARTBEAT = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}) INFO  FATAL \\[nioEventLoopGroup-\\d+-\\d+\\] [a-zA-Z0-9)(:_.]+ -Send heartbeat to client channel\\(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+\\)\\[[nul0-9.]+/([a-zA-Z0-9]+) : ([a-zA-Z0-9]+)\\]$");
        private static final Pattern RECEIVE_HEARTBEAT = Pattern.compile("^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}) INFO  FATAL \\[nioEventLoopGroup-\\d+-\\d+\\] [a-zA-Z0-9)(:_.]+ -Receive heartbeat to client channel\\(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+\\)\\[[nul0-9.]+/([a-zA-Z0-9]+) : ([a-zA-Z0-9]+)\\] .+$");
        private static final Pattern USER_EVENT_TRIGGERED = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.\\d{3}) INFO  FATAL \\[nioEventLoopGroup-\\d+-\\d+\\] [a-zA-Z0-9)(:_.]+ -userEventTriggered channel\\(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+\\)\\[[nul0-9.]+/([a-zA-Z0-9]+) : ([a-zA-Z0-9]+)\\] on netty server$");

        private static final String FIELD_SEPARATOR = "|";

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            outKey.clear();
            outValue.clear();

            String valueStr = value.toString();
//            Matcher userEventTriggeredMatcher = USER_EVENT_TRIGGERED.matcher(valueStr);
//            if (userEventTriggeredMatcher.find()) {// 不处理，不使用
//                return;
//            }
            Matcher multipleChannelMatcher = MULTIPLE_CHANNEL.matcher(valueStr);
            if (multipleChannelMatcher.find()) {
                String time = multipleChannelMatcher.group(1);
                String mac = multipleChannelMatcher.group(2);
                if ("null".equals(mac)) {// 没有注册机顶盒不处理，不使用
                    return;
                }
                String cid = multipleChannelMatcher.group(3);
                String sno = multipleChannelMatcher.group(4);
                outKey.set(cid + FIELD_SEPARATOR + mac + FIELD_SEPARATOR + "MULTIPLE");
                outValue.set(cid + FIELD_SEPARATOR + mac + FIELD_SEPARATOR + "MULTIPLE" + FIELD_SEPARATOR + time + FIELD_SEPARATOR + sno);
                context.write(outKey, outValue);
                return;
            }
            Matcher sendHeartbeatChannelMatcher = SEND_HEARTBEAT.matcher(valueStr);
            if (sendHeartbeatChannelMatcher.find()) {
                String time = sendHeartbeatChannelMatcher.group(1);
                String mac = sendHeartbeatChannelMatcher.group(2);
                if ("null".equals(mac)) {// 没有注册机顶盒不处理，不使用
                    return;
                }
                String cid = sendHeartbeatChannelMatcher.group(3);
                outKey.set(cid + FIELD_SEPARATOR + mac + FIELD_SEPARATOR + "SEND_HEART");
                outValue.set(cid + FIELD_SEPARATOR + mac + FIELD_SEPARATOR + "SEND_HEART" + FIELD_SEPARATOR + time);
                context.write(outKey, outValue);
                return;
            }
            Matcher receiveHeartbeatZookeeperMatcher = RECEIVE_HEARTBEAT.matcher(valueStr);
            if (receiveHeartbeatZookeeperMatcher.find()) {
                String cid = receiveHeartbeatZookeeperMatcher.group(1);
                String mac = receiveHeartbeatZookeeperMatcher.group(2);
                if ("null".equals(mac)) {// 没有注册机顶盒不处理，不使用
                    return;
                }
                String info = receiveHeartbeatZookeeperMatcher.group(3);
                outKey.set(cid + FIELD_SEPARATOR + mac + FIELD_SEPARATOR + "RECEIVE_HEART");
                outValue.set(cid + FIELD_SEPARATOR + mac + FIELD_SEPARATOR + "RECEIVE_HEART" + FIELD_SEPARATOR + info);
                context.write(outKey, outValue);
                return;
            }
//            System.out.println("Mapper " + value);
        }

    }

    public static class _Reducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                for (Text _value : value) {
                    if (_value == null) {
                        continue;
                    }
                    context.write(_value, NullWritable.get());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    public static class Main {


        /**
         * 主方法。
         *
         * @param args 参数列表。参数名：
         *             usage: Options
         *             -h,--help             Print options' information
         *             -i,--input <args>     The paths of data input
         *             -jn,--jobName <arg>   The name of job
         *             -o,--output <arg>     The path of data output
         * @throws Exception 异常
         */
        public static void main(String[] args) throws Exception {
            Configuration configuration = new Configuration();
            ToolRunner.run(configuration, new ConvertFatalData(), args);
        }
    }

    public static class OozieMain {


        /**
         * 主方法。
         *
         * @param args 参数列表。参数名：
         *             usage: Options
         *             -h,--help             Print options' information
         *             -i,--input <args>     The paths of data input
         *             -jn,--jobName <arg>   The name of job
         *             -o,--output <arg>     The path of data output
         * @throws Exception 异常
         */
        public static void main(String[] args) throws Exception {
            Configuration configuration = LauncherMain.loadActionConf();
            ToolRunner.run(configuration, new ConvertFatalData(), args);
        }
    }
}
