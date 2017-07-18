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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.*;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class MobileLogDuration extends Configured implements Tool {


    private static class MobileMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String msg = value.toString();
                Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(msg);
                if (!matcher.find()) {
                    return;
                }
//                if (!StringUtils.isBlank(matcher.group(6))&&matcher.group(6).equals("start")){
//                    starts.put(matcher.group(2),msg);
//
//                }
//                if (!StringUtils.isBlank(matcher.group(6))&&matcher.group(6).equals("end")){
//                    ends.put(matcher.group(2),msg);
//                }

                context.write(new Text(matcher.group(1)), value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {

        private HBaseHelper hBaseHelper;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.hBaseHelper = new HBaseHelper(conf);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {

                Configuration conf = context.getConfiguration();
                Iterator<Text> textIterator = value.iterator();
                TargetUserReadBean targetUserReadBean = new TargetUserReadBean();
                TargetUserReadAttrBean targetUserReadAttrBean = new TargetUserReadAttrBean();
                TargetUserReadRelaBean targetUserReadRelaBean = new TargetUserReadRelaBean();
                String timestemps =null;
                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    SourceMobileBean sourceMobileBean = new SourceMobileBean(rowLineContent);
                    targetUserReadBean.setRowKey(sourceMobileBean.getMobileId()+"|"+sourceMobileBean.getUuid().substring(0,10));
                    this.setForAttrBean(conf,targetUserReadAttrBean, sourceMobileBean);
                    this.setForRelaBean(conf,targetUserReadRelaBean, sourceMobileBean);

                }

                targetUserReadBean.setTargetUserReadRelaBean(targetUserReadRelaBean);
                targetUserReadBean.setTargetUserReadAttrBean(targetUserReadAttrBean);
                hBaseHelper.insert(targetUserReadBean);


            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void setForAttrBean(Configuration conf, TargetUserReadAttrBean bean, SourceMobileBean source) throws Exception {
            bean.setDeviceId(source.getMobileId());
            if (StringUtils.isBlank(bean.getStart())){
                bean.setStart(source.getTimestamps());
            }else if (Long.valueOf(bean.getStart())<Long.valueOf(source.getTimestamps())){
                bean.setEnd(source.getTimestamps());
            }else if (Long.valueOf(bean.getStart())>Long.valueOf(source.getTimestamps())){
                bean.setEnd(bean.getStart());
                bean.setStart(source.getTimestamps());

            }
            bean.setConId(source.getContentId());

            //读取mysql
            readMysqlContent(conf.get("hdfsCluster"));
            Content content = (Content) MysqlCommonVariables.modelMap.get(source.getContentId());
            if (null!=content){
                bean.setConNam(content.getTitle());
                bean.setContent(content.getContent());
            }

            if (!(StringUtils.isBlank(bean.getStart())||StringUtils.isBlank(bean.getEnd()))){
                Long duration = Long.valueOf(bean.getEnd()) - Long.valueOf(bean.getStart());
                bean.setVTime(duration.toString());
            }

            bean.setLongitude(source.getLongitude());
            bean.setLatitude(source.getLatitude());
            bean.setOsType(source.getOsType());
        }

        private void setForRelaBean(Configuration conf, TargetUserReadRelaBean bean, SourceMobileBean source) throws Exception {

            bean.setDeviceId(source.getMobileId());
            bean.setCatId(source.getCategoryId());
                if (source.getCategoryId().equals("-1")){
                    bean.setCatName("热点");
                }
                else if (source.getCategoryId().equals("-2")){
                    bean.setCatName("点播");
                }
                else {

                    //读取mysql
                    readMysqlCategory(conf.get("hdfsCluster"));
                    Category category = (Category) MysqlCommonVariables.modelMap.get(source.getCategoryId());
                    if (null!=category){
                        bean.setCatName(category.getName());
                    }

                }
            String reg = "[0-9]+";
            if (!source.getHotelId().matches(reg)){
                bean.setHotel("");
            }else {
                bean.setHotel(source.getHotelId());
            }

            //读取mysql
            readMysqlHotel(conf.get("hdfsCluster"));
            Hotel hotel = (Hotel) MysqlCommonVariables.modelMap.get(source.getHotelId());
            if(hotel != null) {
                bean.setHotelName(hotel.getName());
            }

            if (!source.getRoomId().matches(reg)){
                bean.setRoom("");
            }else {
                bean.setRoom(source.getRoomId());
            }
            //读取mysql
            readMysqlRoom(conf.get("hdfsCluster"));
            Room room = (Room) MysqlCommonVariables.modelMap.get(source.getHotelId());
            if (null!=room){
                bean.setRoomName(room.getName());
            }

        }

        /**
         * 查询酒店信息
         * @param hdfsCluster
         * @throws Exception
         */
        public void readMysqlHotel(String hdfsCluster) throws Exception{
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(Hotel.class);
            selectModel.setQuery("select id,name from savor_hotel");
            selectModel.setCountQuery("select count(*) from savor_hotel");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

            JdbcReader.readToMap(hdfsCluster,selectModel);

        }

        /**
         * 查询包间信息
         * @param hdfsCluster
         * @throws Exception
         */
        public void readMysqlRoom(String hdfsCluster) throws Exception{
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(Room.class);
            selectModel.setQuery("select id,name from savor_room");
            selectModel.setCountQuery("select count(*) from savor_room");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

            JdbcReader.readToMap(hdfsCluster,selectModel);

        }

        /**
         * 查询包间信息
         * @param hdfsCluster
         * @throws Exception
         */
        public void readMysqlContent(String hdfsCluster) throws Exception{
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(Content.class);
            selectModel.setQuery("select id,title,content from savor_mb_content");
            selectModel.setCountQuery("select count(*) from savor_mb_content");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

            JdbcReader.readToMap(hdfsCluster,selectModel);

        }

        /**
         * 查询包间信息
         * @param hdfsCluster
         * @throws Exception
         */
        public void readMysqlCategory(String hdfsCluster) throws Exception{
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(Category.class);
            selectModel.setQuery("select id,name from savor_mb_category");
            selectModel.setCountQuery("select count(*) from savor_mb_category");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

            JdbcReader.readToMap(hdfsCluster,selectModel);

        }
    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsInputStart = CommonVariables.getParameterValue(Argument.InputPathStart);
            String hdfsInputEnd = CommonVariables.getParameterValue(Argument.InputPathEnd);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), MobileLogDuration.class.getSimpleName());
            job.setJarByClass(MobileLogDuration.class);

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

            /**作业输入*/
            Path inputPath1 = new Path(hdfsInputStart);
            FileInputFormat.addInputPath(job, inputPath1);
            Path inputPath2 = new Path(hdfsInputEnd);
            FileInputFormat.addInputPath(job, inputPath2);
            job.setMapperClass(MobileMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(MobileReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

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

    public static boolean isYesterday(long time) {
        boolean isYesterday = false;
        Date date;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            date = sdf.parse(sdf.format(new Date()));
            if (time < date.getTime() && time > (date.getTime() - 24*60*60*1000)) {
                isYesterday = true;
            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return isYesterday;
    }
}
