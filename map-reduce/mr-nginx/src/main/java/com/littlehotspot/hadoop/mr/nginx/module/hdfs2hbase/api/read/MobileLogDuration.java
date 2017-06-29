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
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.SourceUserBean;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.TargetUserAttrBean;
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
import java.util.*;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class MobileLogDuration extends Configured implements Tool {

    private static Map<String,String> starts;

    private static Map<String,String> ends;

    private static List<HashMap<String, Object>> hotels;

    private static List<HashMap<String, Object>> rooms;

    private static List<HashMap<String, Object>> contents;

    private static List<HashMap<String, Object>> cates;

    static {
        starts=new HashMap<String, String>();
        ends=new HashMap<String, String>();
        hotels = new HotelService().getAll();
        rooms = new RoomService().getAll();
        contents = new ContentService().getAll();
        cates = new CategoryService().getAll();
    }

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


        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Text> textIterator = value.iterator();
                TargetUserReadAttrBean targetReadBean = new TargetUserReadAttrBean();
                TargetUserReadRelaBean targetReadRelaBean = new TargetUserReadRelaBean();
                String timestemps =null;
                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    SourceMobileBean sourceMobileBean = new SourceMobileBean(rowLineContent);

                    targetReadBean.setRowKey(sourceMobileBean.getMobileId()+sourceMobileBean.getUuid().substring(0,10));
                    targetReadBean.setDeviceId(sourceMobileBean.getMobileId());
                    if (StringUtils.isBlank(timestemps)){
                        targetReadBean.setStart(sourceMobileBean.getTimestamps());
                        timestemps=sourceMobileBean.getTimestamps();
                    }else if (Long.valueOf(timestemps)<Long.valueOf(sourceMobileBean.getTimestamps())){
                        targetReadBean.setEnd(sourceMobileBean.getTimestamps());
                    }else if (Long.valueOf(timestemps)>Long.valueOf(sourceMobileBean.getTimestamps())){
                        targetReadBean.setStart(sourceMobileBean.getTimestamps());
                        targetReadBean.setEnd(timestemps);
                    }
                    targetReadBean.setConId(sourceMobileBean.getContentId());
                        for (HashMap<String, Object> content : contents) {
                            if (content.get("id").toString().equals(sourceMobileBean.getContentId())){
                                targetReadBean.setConNam(content.get("title").toString());
                                targetReadBean.setContent(content.get("content").toString());
                            }
                        }
                    if (!(StringUtils.isBlank(targetReadBean.getStart())||StringUtils.isBlank(targetReadBean.getEnd()))){
                        Long duration = Long.valueOf(targetReadBean.getEnd()) - Long.valueOf(targetReadBean.getStart());
                        targetReadBean.setVTime(duration.toString());
                    }

                    targetReadBean.setLongitude(sourceMobileBean.getLongitude());
                    targetReadBean.setLatitude(sourceMobileBean.getLatitude());
                    targetReadBean.setOsType(sourceMobileBean.getOsType());

                    targetReadRelaBean.setRowKey(sourceMobileBean.getMobileId()+sourceMobileBean.getUuid().substring(0,10));
                    targetReadRelaBean.setDeviceId(sourceMobileBean.getMobileId());
                    targetReadRelaBean.setCatId(sourceMobileBean.getCategoryId());
                    for (HashMap<String, Object> cate : cates) {
                        if (sourceMobileBean.getCategoryId().equals("-1")){
                            targetReadRelaBean.setCatName("热点");
                        }
                        else if (sourceMobileBean.getCategoryId().equals("-2")){
                            targetReadRelaBean.setCatName("点播");
                        }
                        else if (cate.get("id").toString().equals(sourceMobileBean.getCategoryId())){
                            targetReadRelaBean.setCatName(cate.get("name").toString());
                        }
                    }
                    String reg = "[0-9]+";
                    if (!sourceMobileBean.getHotelId().matches(reg)){
                        targetReadRelaBean.setHotel("");
                    }else {
                        targetReadRelaBean.setHotel(sourceMobileBean.getHotelId());
                    }

                    for (HashMap<String, Object> hotel : hotels) {
                        if (hotel.get("id").toString().equals(sourceMobileBean.getHotelId())){
                            targetReadRelaBean.setHotelName(hotel.get("name").toString());
                        }
                    }
                    if (!sourceMobileBean.getRoomId().matches(reg)){
                        targetReadRelaBean.setRoom("");
                    }else {
                        targetReadRelaBean.setRoom(sourceMobileBean.getRoomId());
                    }
                    for (HashMap<String, Object> room : rooms) {
                        if (room.get("id").toString().equals(sourceMobileBean.getRoomId())){
                            targetReadRelaBean.setRoomName(room.get("name").toString());
                        }
                    }
                    CommonVariables.hBaseHelper.insert(targetReadBean);

                    CommonVariables.hBaseHelper.insert(targetReadRelaBean);
                }

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
