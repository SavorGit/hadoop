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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic;

    import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
    import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.CommonVariables;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.conf.Configured;
    import org.apache.hadoop.fs.FileStatus;
    import org.apache.hadoop.fs.FileSystem;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.LongWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.mapreduce.filecache.DistributedCache;
    import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
    import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.util.Tool;

    import java.io.IOException;
    import java.net.URI;

    /**
     * 手机日志
     */
    public class RqFromMysql extends Configured implements Tool {


        private static class MobileMapper extends Mapper<LongWritable, RqUserModel, Text, Text> {


            @Override
            protected void map(LongWritable key, RqUserModel value, Context context) throws IOException, InterruptedException {
                try {
                    System.out.println(value.toString());
                    context.write(new Text(value.toString()), new Text());

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }


        }


        @Override
        public int run(String[] args) throws Exception {
            try {

                CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

                // 获取参数
                String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
                String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
                String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

//                this.getConf().set("mapred.job.tracker", "localhost:9001");
                DBConfiguration.configureDB(this.getConf(), "com.mysql.jdbc.Driver", "jdbc:mysql://rr-2zevja6lfg5718e3ko.mysql.rds.aliyuncs.com:3306/cloud?useSSL=false&useUnicode=true&characterEncoding=utf8&characterSetResults=utf8&zeroDateTimeBehavior=convertToNull", "java_api_read", "KESs23DRZVX7hrqe");

                /**作业输出*/
                Path outputPath = new Path(hdfsOutputPath);

                // 如果输出路径已经存在，则删除
                FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
                if (fileSystem.exists(outputPath)) {
                    fileSystem.delete(outputPath, true);
                }

                Job job = Job.getInstance(this.getConf(), RqFromMysql.class.getSimpleName());
                job.setJarByClass(RqFromMysql.class);


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


                FileOutputFormat.setOutputPath(job, outputPath);
                job.setInputFormatClass(DBInputFormat.class);
                job.setMapperClass(MobileMapper.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                String fieldSQL = "select id,source_type,clientid,deviceid,dowload_device_id,hotelid,waiterid,min(add_time) as add_time from savor_download_count where deviceid is not null group by deviceid";
                String countSQL = "select count(*) from savor_download_count";
                DBInputFormat.setInput(job, RqUserModel.class, fieldSQL, countSQL);
                FileOutputFormat.setOutputPath(job, outputPath);

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
