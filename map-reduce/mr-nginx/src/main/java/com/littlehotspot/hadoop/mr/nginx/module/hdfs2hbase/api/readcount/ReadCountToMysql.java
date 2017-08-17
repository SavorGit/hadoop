    /**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:38
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.readcount;

    import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
    import org.apache.commons.lang.StringUtils;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.conf.Configured;
    import org.apache.hadoop.fs.FileStatus;
    import org.apache.hadoop.fs.FileSystem;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.LongWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.mapreduce.filecache.DistributedCache;
    import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
    import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.util.Tool;

    import java.io.IOException;
    import java.net.URI;
    import java.util.Iterator;
    import java.util.regex.Matcher;
    import java.util.regex.Pattern;

    /**
     * 手机日志
     */
    public class ReadCountToMysql extends Configured implements Tool {


        private static class MobileMapper extends Mapper<LongWritable, Text, Text, Text> {


            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                try {
                    String msg = value.toString();
                    Matcher matcher = CommonVariable.MAPPER_FORMAT_REGEX.matcher(msg);
                    if (!matcher.find()) {
                        return;
                    }


                    context.write(new Text(matcher.group(1)), new Text(msg));

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }


        }

        private static class DBOutputReducer extends Reducer<Text, Text, ReadCountModel, Text> {


            @Override
            protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
                try {

                Iterator<Text> textIterator = value.iterator();
                ReadCountModel model = new ReadCountModel();

                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    Matcher matcher = Pattern.compile("^(.*),(.*),(.*)$").matcher(item.toString());
                    if (!matcher.find()) {
                        return;
                    }
                    model.setConId(matcher.group(1));
                    model.setConName(matcher.group(2));
                    if (StringUtils.isBlank(model.getCount())){
                        model.setCount(matcher.group(3).trim());
                    }else {
                        Long l = Long.valueOf(model.getCount()) + Long.valueOf(matcher.group(3));
                        model.setCount(l.toString());
                    }


                }

                context.write(model, new Text());
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

        @Override
        public int run(String[] args) throws Exception {
            try {

                CommonVariable.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

                // 获取参数
                String hbaseSharePath = CommonVariable.getParameterValue(Argument.HBaseSharePath);
                String hdfsCluster = CommonVariable.getParameterValue(Argument.HDFSCluster);
                String hdfsInputPath = CommonVariable.getParameterValue(Argument.InputPath);
    //            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);



                this.getConf().set("mapred.job.tracker", "localhost:9001");
                DBConfiguration.configureDB(this.getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.2.145:3306/cloud?useSSL=false&useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&zeroDateTimeBehavior=convertToNull", "javaweb", "123456");

                Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
                job.setJarByClass(this.getClass());


                // 避免报错：ClassNotFoundError hbaseConfiguration
                Configuration jobConf = job.getConfiguration();
                FileSystem hdfs = FileSystem.get(new URI(hdfsCluster), jobConf);
                Path hBaseSharePath = new Path(hbaseSharePath);
                FileStatus[] hBaseShareJars = hdfs.listStatus(hBaseSharePath);
                for (FileStatus fileStatus : hBaseShareJars) {
                    if (!fileStatus.isFile()) {
                        continue;
                    }
                    Path archive = fileStatus.getPath();
                    FileSystem fs = archive.getFileSystem(jobConf);
                    DistributedCache.addArchiveToClassPath(archive, jobConf, fs);
                }//

                Path path = new Path(hdfsInputPath);
                FileInputFormat.setInputPaths(job,path);
                job.setMapperClass(MobileMapper.class);
                job.setReducerClass(DBOutputReducer.class);
                job.setOutputFormatClass(DBOutputFormat.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                DBOutputFormat.setOutput(job, "savor_read_count", "content_id","content_name", "count");

                boolean status = job.waitForCompletion(true);
                if (!status) {
                    throw new Exception("MapReduce task execute failed.........");
                }


                return 0;
            } catch (Exception e) {
                e.printStackTrace();
                return 1;
            }
        }

    }
