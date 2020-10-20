/**
 * Copyright (c) 2020, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 17:43
 */
package com.littlehotspot.hadoop.mr.box;

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

import java.io.IOException;
import java.util.Arrays;

/**
 * 计算开机费用
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2020年10月14日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class StartUpFee extends Configured implements Tool {

    public static class StartUpFeeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            System.out.println(value);
            System.out.println(context);
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }
    }

    public class StartUpFeeReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                context.write(key, NullWritable.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Jar box start up fee action configuration");
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


//        Option property = OptionBuilder.withArgName("property=name")
//                .hasArgs()
//                .withValueSeparator()
//                .withDescription("use value for a property")
//                .create("D");
//        options.addOption(property);

        String _jobName = null;
        String[] _inputPaths = null;
        String _outputPath = null;


        // 解析参数
        CommandLineParser parser = new GnuParser();
//        try {
        CommandLine cli = parser.parse(options, args);
        if (cli.hasOption("h")) {
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
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println("\tJob name    : " + _jobName);
        System.out.println("\tInput paths : " + Arrays.asList(_inputPaths));
        System.out.println("\tOutput path : " + _outputPath);


        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking box start up fee job task now >>>");
        System.out.println();
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
        job.setMapperClass(StartUpFeeMapper.class);// 如果此处不设置的话，可以通过mapreduce.job.map.class来设置。
//        job.setMapOutputKeyClass(Text.class);// 如果此处不设置的话，可以通过mapreduce.map.output.key.class来设置。
//        job.setMapOutputValueClass(NullWritable.class);// 如果此处不设置的话，可以通过mapreduce.map.output.value.class来设置。
//        job.setInputFormatClass(CombineTextInputFormat.class);// 如果此处不设置的话，可以通过mapreduce.job.inputformat.class来设置，配合mapreduce.input.fileinputformat.split.maxsize使用。

//        Path[] inputPaths = new Path[_inputPaths.length];
//        for (int index = 0; index < _inputPaths.length; index++) {
//            inputPaths[index] = new Path(_inputPaths[index]);
//        }
//        FileInputFormat.setInputPaths(job, inputPaths);// 如果此处不设置的话，可以通过mapreduce.input.fileinputformat.inputdir来设置。
//        job.setMapperClass(StartUpFeeMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(NullWritable.class);


        // 作业输出
        Path outputPath = new Path(_outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);// 如果此处不设置的话，可以通过mapreduce.output.fileoutputformat.outputdir来设置。
        job.setReducerClass(StartUpFeeReducer.class);// 如果此处不设置的话，可以通过mapreduce.job.reduce.class来设置。
        job.setOutputKeyClass(Text.class);// 如果此处不设置的话，可以通过mapreduce.job.output.key.class来设置。
        job.setOutputValueClass(NullWritable.class);// 如果此处不设置的话，可以通过mapreduce.job.output.value.class来设置。

//        job.setJar("E:\\WorkSpace\\Company\\Savor\\Git\\JAVA\\hadoop\\map-reduce\\box-statistics\\start-up\\target\\start-up-LHS.HADOOP.2.11.1.0.1.0.0-SNAPSHOT.jar");
        boolean status = job.waitForCompletion(true);
        if (!status) {
            String exceptionMessage = String.format("MapReduce[Box-StartUp-Fee] task[%s] execute failed", _jobName);
            throw new Exception(exceptionMessage);
        }
        return 0;
    }


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
        ToolRunner.run(configuration, new StartUpFee(), args);
    }
}
