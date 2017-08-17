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
    import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
    import net.lizhaoweb.spring.hadoop.commons.utils.IJDBCTools;
    import net.lizhaoweb.spring.hadoop.commons.utils.impl.JDBCTools;
    import org.apache.commons.lang.StringUtils;
    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.conf.Configured;
    import org.apache.hadoop.fs.FileStatus;
    import org.apache.hadoop.fs.FileSystem;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.hbase.client.Result;
    import org.apache.hadoop.hbase.client.Scan;
    import org.apache.hadoop.hbase.filter.CompareFilter;
    import org.apache.hadoop.hbase.filter.Filter;
    import org.apache.hadoop.hbase.filter.FilterList;
    import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
    import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
    import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
    import org.apache.hadoop.hbase.mapreduce.TableMapper;
    import org.apache.hadoop.hbase.util.Bytes;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.mapreduce.filecache.DistributedCache;
    import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
    import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
    import org.apache.hadoop.util.Tool;

    import java.io.IOException;
    import java.net.URI;
    import java.text.SimpleDateFormat;
    import java.util.ArrayList;
    import java.util.Calendar;
    import java.util.Iterator;
    import java.util.List;
    import java.util.regex.Matcher;
    import java.util.regex.Pattern;

/**
 * 手机日志
 */
public class InToMysql extends Configured implements Tool {


    private static class MobileMapper extends TableMapper<Text, Text> {


        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
            try {
                String row = Bytes.toString(result.getRow());
                String areaId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area_id")));
                String areaName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area_name")));
                String hotelId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_id")));
                String hotelName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name")));
                String roomId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_id")));
                String roomName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name")));
                String boxId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("box_id")));
                String boxName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("box_name")));
                String mac = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac")));
                String mediaId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("media_id")));
                String mediaName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("media_name")));
                String playTime = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_time")));
                String playCount = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_count")));
                String playDate = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_date")));
                SourceBean bean = new SourceBean();
                bean.setRowKey(row);
                bean.setAreaId(areaId);
                bean.setArea(areaName);
                bean.setHotelId(hotelId);
                bean.setHotelName(hotelName);
                bean.setRoomId(roomId);
                bean.setRoomName(roomName);
                bean.setBoxId(boxId);
                bean.setBoxName(boxName);
                bean.setMac(mac);
                bean.setMediaId(mediaId);
                bean.setMediaName(mediaName);
                bean.setPlayTime(playTime);
                bean.setPlayCount(playCount);
                bean.setPlayDate(playDate);
//        System.out.println("ROWKEY{"+row+"}"+":mda_type="+mediaType+":option_type="+optionType+":mda_id="+mediaId);


                context.write(new Text(row), new Text(bean.rowLine2()));

            } catch (Exception e) {
                e.printStackTrace();
            }

        }


    }

    private static class DBOutputReducer extends Reducer<Text, Text, MediaStaModel, Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, MediaStaModel, Text>.Context context) throws IOException, InterruptedException {
            try {

            Iterator<Text> textIterator = value.iterator();
            MediaStaModel model = new MediaStaModel();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                Matcher matcher = Pattern.compile("^(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$").matcher(item.toString());
                if (!matcher.find()) {
                    return;
                }

                model.setRowKey(matcher.group(1));
                try {
                    model.setAreaId(Integer.parseInt(matcher.group(2)));
                }catch (Exception e){
                    e.printStackTrace();
                    model.setAreaId(0);
                }
                try {
                    model.setHotelId(Long.valueOf(matcher.group(4)));
                }catch (Exception e){
                    e.printStackTrace();
                    model.setHotelId(0);
                }
                try {
                    model.setRoomId(Long.valueOf(matcher.group(6)));
                }catch (Exception e){
                    e.printStackTrace();
                    model.setRoomId(0);
                }
                try {
                    model.setBoxId(Long.valueOf(matcher.group(8)));
                }catch (Exception e){
                    e.printStackTrace();
                    model.setBoxId(0);
                }
               try {
                   model.setMediaId(Long.valueOf(matcher.group(11)));
               }catch (Exception e){
                   model.setMediaId(0);
               }
               try {
                   model.setPlayCount(Integer.parseInt(matcher.group(13)));
               }catch (Exception e){
                   e.printStackTrace();
                   model.setPlayCount(0);
               }
                try {
                    model.setPlayTime(Integer.parseInt(matcher.group(14)));
                }catch (Exception e){
                    e.printStackTrace();
                    model.setPlayTime(0);
                }
                try {
                    model.setPlayDate(Integer.parseInt(matcher.group(15).trim()));
                }catch (Exception e){
                    e.printStackTrace();
                    model.setPlayDate(0);
                }
                model.setAreaName(matcher.group(3));
                model.setHotelName(matcher.group(5));
                model.setRoomName(matcher.group(7));
                model.setBoxName(matcher.group(9));
                model.setMac(matcher.group(10));
                model.setMediaName(matcher.group(12));

            }
//            Matcher matcher = MEDIA_PATTERN.matcher(line);
//            if (!matcher.find()) {
//                return;
//            }
            context.write(model, new Text());
            }catch (Exception e){
                e.printStackTrace();
            }
//            context.write(r, new Text(r.getName()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsInputPath = CommonVariables.getParameterValue(Argument.InputPath);
            String jdbcUrl = CommonVariables.getParameterValue(Argument.JdbcUrl);
            String mysqlUser = CommonVariables.getParameterValue(Argument.MysqlUser);
            String mysqlPassWord = CommonVariables.getParameterValue(Argument.MysqlPassWord);
            String before = CommonVariables.getParameterValue(Argument.Before);
            String time = CommonVariables.getParameterValue(Argument.Time);
            String sql = CommonVariables.getParameterValue(Argument.Sql);
//            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

//            JDBCTool jdbcTool = new JDBCTool("com.mysql.jdbc.Driver",jdbcUrl,mysqlUser,mysqlPassWord);
            IJDBCTools jdbcTool = new JDBCTools("com.mysql.jdbc.Driver",jdbcUrl,mysqlUser,mysqlPassWord) ;

//            this.getConf().set("mapred.job.tracker", "localhost:9001");
            DBConfiguration.configureDB(this.getConf(), "com.mysql.jdbc.Driver", jdbcUrl, mysqlUser, mysqlPassWord);

            Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
            job.setJarByClass(this.getClass());

            Scan scan = new Scan();
            //设置过滤器
            List<Filter> filters= new ArrayList<Filter>();
            if (!StringUtils.isBlank(time)&&!StringUtils.isBlank(before)){
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                Calendar now =Calendar.getInstance();
                now.setTime(format.parse(time));
                now.set(Calendar.DATE,now.get(Calendar.DATE)-Integer.parseInt(before));
                String day = format.format(now.getTime());
                jdbcTool.executeUpdate(sql,Integer.parseInt(day),Integer.parseInt(time));
//                jdbcTool.updateSQL(sql,Integer.parseInt(day),Integer.parseInt(time));
                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(day+"00"));
                filters.add(timefilter);
            }

            if(null==scan) {
                System.out.println("error : scan = null");
                return 1;
            }

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

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
            TableMapReduceUtil.initTableMapperJob("media_sta", scan, MobileMapper.class, Text.class, Text.class, job,false);

            /**作业输出*/
//            Path outputPath = new Path(hdfsOutputPath);
//            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
//            if (fileSystem.exists(outputPath)) {
//                fileSystem.delete(outputPath, true);
//            }

//            FileOutputFormat.setOutputPath(job, outputPath);
            job.setMapperClass(MobileMapper.class);
            job.setReducerClass(DBOutputReducer.class);
            job.setOutputFormatClass(DBOutputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            DBOutputFormat.setOutput(job, "savor_medias_sta", "rowKey","area_id", "area_name","hotel_id", "hotel_name","room_id", "room_name", "box_id","box_name","mac", "media_id","media_name", "play_time", "play_count", "play_date");

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
