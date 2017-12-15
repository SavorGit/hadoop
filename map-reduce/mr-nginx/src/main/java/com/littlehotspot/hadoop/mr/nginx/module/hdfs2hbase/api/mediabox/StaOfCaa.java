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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate.BoxTableMapper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read.*;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.*;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import com.littlehotspot.hadoop.mr.nginx.util.JSONUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class StaOfCaa extends Configured implements Tool {



    private static class MobileMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            String row = Bytes.toString(result.getRow());
            String hotelId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_id")));
            String hotelName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name")));
            String roomId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_id")));
            String roomName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name")));
            String mac = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac")));
            String mediaId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_id")));
            String mediaType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_type")));
            String optionType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type")));
            String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
            SourceBean sourceBean = new SourceBean();
            sourceBean.setHotelId(hotelId);
            sourceBean.setHotelName(hotelName);
            sourceBean.setRoomId(roomId);
            sourceBean.setRoomName(roomName);
            sourceBean.setMac(mac);
            sourceBean.setMediaId(mediaId);
            sourceBean.setMediaType(mediaType);
            sourceBean.setPlayDate(stampToDate(timestamps));
            sourceBean.setTimestamps(timestamps);
            try {
                if (!StringUtils.isBlank(mediaId)){
                    context.write(new Text(sourceBean.getHotelId()+"|"+sourceBean.getRoomId()+"|"+sourceBean.getMac()+"|"+sourceBean.getMediaId()+"|"+sourceBean.getPlayDate()), new Text(sourceBean.rowLine1()));
                }else {
                    return;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public static String stampToDate(String s){
            String res;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            return res;
        }
    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {

        private HBaseHelper hBaseHelper;

        private ResourceType resourceType;

        private Map<String, Object> hotelMap = new ConcurrentHashMap<>();

        private Map<String, Object> areaMap = new ConcurrentHashMap<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.hBaseHelper = new HBaseHelper(conf);

            List<Result> hotels = hBaseHelper.getAllRecord("hotel");
            for (Result result : hotels) {

                hotelMap.put(new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("id"))),new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area_id"))));
            }

            List<Result> areas = hBaseHelper.getAllRecord("area");
            for (Result result : areas) {

                areaMap.put(new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("id"))),new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("region_name"))));
            }

        }
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {

                Configuration conf = context.getConfiguration();
                Iterator<Text> textIterator = value.iterator();
                SourceBean bean = new SourceBean();
                Integer count =0;
                bean.setRowKey(key.toString());
                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(rowLineContent);
                    if (!matcher.find()){
                        return;
                    }

                    bean.setHotelName(matcher.group(2));
                    bean.setRoomId(matcher.group(3));
                    bean.setRoomName(matcher.group(4));
                    bean.setMac(matcher.group(5));
                    bean.setPlayDate(StringUtils.trim(matcher.group(8)));
                    bean.setHotelId(matcher.group(1));
                    if (StringUtils.isBlank(matcher.group(1))){
                        return;
                    }
                    if (StringUtils.isBlank(bean.getArea())){
                        String areaId = (String)hotelMap.get(matcher.group(1));
                        String areaName = (String)areaMap.get(areaId);
                        bean.setArea(areaName);
                        bean.setAreaId(areaId);
                    }

                    Result medias=hBaseHelper.getOneRecord("medias", matcher.group(6));

                    if (medias.isEmpty()){
                        System.out.println(item.toString() + ": RESULT IS EMPTY");
                        return;
                    }
                    bean.setMediaId(matcher.group(6));

                    String duration = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("duration")));
                    if (StringUtils.isBlank(bean.getPlayTime())){
                        bean.setPlayTime(duration);
                    }else {
                        Long playtime =Long.valueOf(bean.getPlayTime())+Long.valueOf(duration);
                        bean.setPlayTime(playtime.toString());
                    }
                    String mediaName = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("name")));
                    bean.setMediaName(mediaName);


                    count++;

                }
                bean.setPlayCount(count.toString());
                context.write(new Text(bean.rowLine2()), new Text());

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
            String time = CommonVariables.getParameterValue(Argument.Time);
            String before = CommonVariables.getParameterValue(Argument.Before);

            Job job = Job.getInstance(this.getConf(), StaOfCaa.class.getSimpleName());
            job.setJarByClass(StaOfCaa.class);

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



            Scan scan = new Scan();

            //设置过滤器
            List<Filter> filters= new ArrayList<Filter>();

            RegexStringComparator comp = new RegexStringComparator("^(ads)|(pro)|(adv)$");
            SingleColumnValueFilter typefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("mda_type"), CompareFilter.CompareOp.EQUAL,comp);
            SingleColumnValueFilter optionfilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("option_type"), CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("start")));
//            if (!StringUtils.isBlank(time)){
//                RegexStringComparator comps = new RegexStringComparator("^"+"time");
//                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
//                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.EQUAL,comps);
//                filters.add(timefilter);
//            }

            if (!StringUtils.isBlank(time)&&!StringUtils.isBlank(before)){
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                Calendar now =Calendar.getInstance();
                now.setTime(format.parse(time));
                now.set(Calendar.DATE,now.get(Calendar.DATE)-Integer.parseInt(before));
                String day = format.format(now.getTime());
                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(day+"00"));
                filters.add(timefilter);
            }else if (!StringUtils.isBlank(time)&&StringUtils.isBlank(before)){
                RegexStringComparator comps = new RegexStringComparator("^"+"time");
                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.EQUAL,comps);
                filters.add(timefilter);
            }

            if(null==scan) {
                System.out.println("error : scan = null");
                return 1;

            }

            filters.add(typefilter);
            filters.add(optionfilter);

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
            TableMapReduceUtil.initTableMapperJob("box_log", scan, MobileMapper.class, Text.class, Text.class, job,false);


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
