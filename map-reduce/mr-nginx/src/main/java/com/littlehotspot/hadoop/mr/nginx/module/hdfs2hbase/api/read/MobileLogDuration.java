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
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.ICategoryMapper;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IContentMapper;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IHotelMapper;
import com.littlehotspot.hadoop.mr.nginx.mysql.mapper.IRoomMapper;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.CategoryService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.ContentService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.HotelService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.RoomService;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class MobileLogDuration extends Configured implements Tool {

    private static Map<String,String> starts;

    private static Map<String,String> ends;

    static {
        starts=new HashMap<String, String>();
        ends=new HashMap<String, String>();
    }

    private static class MobileMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String msg = value.toString();
                Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(msg);
                if (!matcher.find()) {
                    return;
                }
                String option = matcher.group(6);
                if (!StringUtils.isBlank(option)&&option.equals("start")){
                    starts.put(matcher.group(2),msg);

                }
                if (!StringUtils.isBlank(option)&&option.equals("end")){
                    ends.put(matcher.group(2),msg);
                }

//                System.out.println(value.toString());
                context.write(new Text(), new Text());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                List<HashMap<String, Object>> hotels = new HotelService().getAll();
                List<HashMap<String, Object>> rooms = new RoomService().getAll();
                List<HashMap<String, Object>> contents = new ContentService().getAll();
                List<HashMap<String, Object>> cates = new CategoryService().getAll();
                for (String start : starts.keySet()) {
                    for (String end : ends.keySet()) {
                        if (start.equals(end)){
                            String s = starts.get(start);
                            Matcher matcherStart = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(s);

                            Matcher matcherEnd = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(ends.get(end));
                            if (!matcherStart.find()) {
                                return;
                            }
                            if (!matcherEnd.find()) {
                                return;
                            }
                            Long duation = Long.valueOf(matcherEnd.group(5)) - Long.valueOf(matcherStart.group(5));

                            TargetUserReadAttrBean targetReadBean = new TargetUserReadAttrBean();
                            TargetUserReadRelaBean targetReadRelaBean = new TargetUserReadRelaBean();
                            String deviceId = matcherStart.group(10);
                            targetReadBean.setRowKey(matcherStart.group(10)+matcherStart.group(5).substring(0,10));
                            targetReadBean.setDeviceId(matcherStart.group(10));
                            targetReadBean.setStart(matcherStart.group(5));
                            targetReadBean.setEnd(matcherEnd.group(5));
                            targetReadBean.setConId(matcherStart.group(8));
                                for (HashMap<String, String> content : contents) {
                                    if (content.get("id").toString().equals(matcherStart.group(8))){
                                        targetReadBean.setConNam(content.get("title"));
                                        targetReadBean.setContent(content.get("content"));
                                    }
                                }
                            Long duration = Long.valueOf(matcherEnd.group(5)) - Long.valueOf(matcherStart.group(5));
                            targetReadBean.setVTime(duration.toString());
                            targetReadBean.setLongitude(matcherStart.group(13));
                            targetReadBean.setLatitude(matcherStart.group(14));
                            targetReadBean.setOsType(matcherStart.group(12));

                            targetReadRelaBean.setRowKey(matcherStart.group(10)+matcherStart.group(5).substring(0,10));
                            targetReadRelaBean.setDeviceId(matcherStart.group(10));
                            targetReadRelaBean.setStart(matcherStart.group(5));
                            targetReadRelaBean.setCatId(matcherStart.group(9));
                            for (HashMap<String, String> cate : cates) {
                                if (cate.get("id").toString().equals(matcherStart.group(9))){
                                    targetReadRelaBean.setCatName(cate.get("name"));
                                }
                            }
                            targetReadRelaBean.setHotel(matcherStart.group(3));
                            for (HashMap<String, String> hotel : hotels) {
                                if (hotel.get("id").toString().equals(matcherStart.group(3))){
                                    targetReadRelaBean.setHotelName(hotel.get("name"));
                                }
                            }
                            targetReadRelaBean.setRoom(matcherStart.group(4));
                            for (HashMap<String, String> room : rooms) {
                                if (room.get("id").toString().equals(matcherStart.group(4))){
                                    targetReadRelaBean.setRoomName(room.get("name"));
                                }
                            }
                            CommonVariables.hBaseHelper.insert(targetReadBean);

                            CommonVariables.hBaseHelper.insert(targetReadRelaBean);
                        }
                    }
                }
//                context.write(key, new Text());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE
            CommonVariables.hBaseHelper = new HBaseHelper(this.getConf());

            // 获取参数
            String matcherRegex = CommonVariables.getParameterValue(Argument.MapperInputFormatRegex);
            String hdfsInputStart = CommonVariables.getParameterValue(Argument.InputPathStart);
            String hdfsInputEnd = CommonVariables.getParameterValue(Argument.InputPathEnd);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            Job job = Job.getInstance(this.getConf(), MobileLogDuration.class.getSimpleName());
            job.setJarByClass(MobileLogDuration.class);

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
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), new Configuration());
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
