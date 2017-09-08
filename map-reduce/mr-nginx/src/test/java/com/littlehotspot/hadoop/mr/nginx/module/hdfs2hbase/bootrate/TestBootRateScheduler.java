/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:34
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.bootrate;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate.BoxCleanJob;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate.TotalBootRate;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.zhengwei.Validate;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestBootRateScheduler {


    @Test
    public void run() throws IOException, WriteException {
        String[] args = {
                "hdfsCluster=hdfs://onlinemain:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/text-mr/test-box_log",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain"

//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
//                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
//                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        Constant.CommonVariables.initMapReduce(conf, args);
        HBaseHelper hBaseHelper = new HBaseHelper(conf);
//        List<Result> boot_rate = hBaseHelper.getAllRecord("boot_rate");

        HTable table = new HTable(HBaseConfiguration.create(conf), "boot_rate");
        Scan scan = new Scan();
        List<Filter> filters= new ArrayList<Filter>();
        SingleColumnValueFilter start = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                Bytes.toBytes("play_date"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes("2017-08-16"));
        SingleColumnValueFilter end = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                Bytes.toBytes("play_date"), CompareFilter.CompareOp.LESS,Bytes.toBytes("2017-08-31"));
        filters.add(start);
        filters.add(end);

        FilterList filterList = new FilterList(filters);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> boot_rate = new ArrayList<>();
        for (Result result : scanner) {
            boot_rate.add(result);
        }
        scanner.close();


        WritableWorkbook workbook = Workbook.createWorkbook(new File("D:\\2017年8月16-30号开机率明细.xls"));

        WritableSheet sheet = workbook.createSheet("第一页", 0);

        Label area = new Label(0,0,"区域");
        sheet.addCell(area);
        Label hotel = new Label(1,0,"酒楼");
        sheet.addCell(hotel);
        Label addr = new Label(2,0,"位置");
        sheet.addCell(addr);
        Label server = new Label(3,0,"包间");
        sheet.addCell(server);
        Label maintenMan = new Label(4,0,"维护人");
        sheet.addCell(maintenMan);
        Label iskey = new Label(5,0,"重点酒楼");
        sheet.addCell(iskey);
        Label playDate = new Label(6,0,"播放日期");
        sheet.addCell(playDate);
        Label boxMac = new Label(7,0,"机顶盒编号");
        sheet.addCell(boxMac);
        Label playCount = new Label(8,0,"播放次数");
        sheet.addCell(playCount);
        Label playTime = new Label(9,0,"播放总秒数");
        sheet.addCell(playTime);
        Label production = new Label(10,0,"开机率");
        sheet.addCell(production);
//        Map<String,String> map = new HashMap<>();
//        map.put("start","09");
//        map.put("end","10");
//        mapper.insertMiddle(map);
//        mapper.insertTimeOne();
//        mapper.insertTimeTwo();
//        mapper.insertRate();
        for (int i = 0; i < boot_rate.size(); i++) {
            Result result = boot_rate.get(i);
            Label areaName = new Label(0,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area"))));
            sheet.addCell(areaName);
            Label hotelName = new Label(1,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name"))));
            sheet.addCell(hotelName);
            Label address = new Label(2,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("addr"))));
            sheet.addCell(address);
            Label roomName = new Label(3,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name"))));
            sheet.addCell(roomName);
            Label mainten = new Label(4,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mainten_man"))));
            sheet.addCell(mainten);
            Label key = new Label(5,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("isKey"))));
            sheet.addCell(key);
            Label date = new Label(6,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_date"))));
            sheet.addCell(date);
            Label mac = new Label(7,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac"))));
            sheet.addCell(mac);
            Label count = new Label(8,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_count"))));
            sheet.addCell(count);
            Label time = new Label(9,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_time"))));
            sheet.addCell(time);
            Label prod = new Label(10,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("production"))));
            sheet.addCell(prod);
        }



        workbook.write();
        workbook.close();
    }



    @Test
    public void run6() {
        long startTime = System.currentTimeMillis();
        String[] args = {
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-root_rate",
//                "hbaseRoot=hdfs://onlinemain:8020/hbase",
//                "hbaseZookeeper=onlinemain",
//                "hdfsCluster=hdfs://onlinemain:8020",
//                "hbaseSharePath=/user/oozie/share/lib/lib_20170512162404/hbase",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1",
                "hdfsCluster=hdfs://devpd1:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase",
                "startTime=2017071600",
                "endTime=2017073200"


        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {

            ToolRunner.run(conf, new BoxCleanJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("totalTime=======>>>"+(endTime-startTime));
    }

    @Test
    public void total() {
        String[] args = {
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-total_root_rate",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain",
                "hdfsCluster=hdfs://onlinemain:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170512162404/hbase",
//                "hbaseRoot=hdfs://devpd1:8020/hbase",
//                "hbaseZookeeper=devpd1",
//                "hdfsCluster=hdfs://devpd1:8020",
//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase",
                "startTime=2017-08-16",
                "endTime=2017-08-31"


//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
//                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
//                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        try {
            ToolRunner.run(conf, new TotalBootRate(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void run2() throws IOException, WriteException {
        String[] args = {
                "hdfsCluster=hdfs://onlinemain:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/text-mr/test-box_log",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain"

//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
//                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
//                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://devpd1:8020");
//        conf.set("fs.defaultFS", "file:///");
//        conf.set("hbase.master", "devpd1:9000");
//        conf.set("hbase.zookeeper.quorum", "devpd1");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.rootdir", "hdfs://devpd1:9000/hbase");
        Constant.CommonVariables.initMapReduce(conf, args);
        HBaseHelper hBaseHelper = new HBaseHelper(conf);
        List<Result> boot_rate = hBaseHelper.getAllRecord("total_boot_rate");
//        HTable table = new HTable(HBaseConfiguration.create(conf), "boot_rate");
//        Scan scan = new Scan();
//        List<Filter> filters= new ArrayList<Filter>();
//        SingleColumnValueFilter start = new SingleColumnValueFilter(Bytes.toBytes("attr"),
//                Bytes.toBytes("play_date"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes("2017-07-01"));
//        SingleColumnValueFilter end = new SingleColumnValueFilter(Bytes.toBytes("attr"),
//                Bytes.toBytes("play_date"), CompareFilter.CompareOp.LESS,Bytes.toBytes("2017-07-16"));
//        filters.add(start);
//        filters.add(end);
//
//        FilterList filterList = new FilterList(filters);
//        scan.setFilter(filterList);
//        ResultScanner scanner = table.getScanner(scan);
//        List<Result> boot_rate = new ArrayList<>();
//        for (Result result : scanner) {
//            boot_rate.add(result);
//        }
//        scanner.close();


        WritableWorkbook workbook = Workbook.createWorkbook(new File("D:\\2017年8月16-30号开机率汇总.xls"));

        WritableSheet sheet = workbook.createSheet("第一页", 0);

        Label area = new Label(0,0,"区域");
        sheet.addCell(area);
        Label hotel = new Label(1,0,"酒楼");
        sheet.addCell(hotel);
        Label addr = new Label(2,0,"位置");
        sheet.addCell(addr);
        Label server = new Label(3,0,"包间");
        sheet.addCell(server);
        Label maintenMan = new Label(4,0,"维护人");
        sheet.addCell(maintenMan);
        Label iskey = new Label(5,0,"重点酒楼");
        sheet.addCell(iskey);
        Label boxMac = new Label(6,0,"机顶盒编号");
        sheet.addCell(boxMac);
        Label playCount = new Label(7,0,"播放天数");
        sheet.addCell(playCount);
        Label playTime = new Label(8,0,"有效率合计");
        sheet.addCell(playTime);
        Label production = new Label(9,0,"平均有效率");
        sheet.addCell(production);

        for (int i = 0; i < boot_rate.size(); i++) {
            Result result = boot_rate.get(i);
            Label areaName = new Label(0,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area"))));
            sheet.addCell(areaName);
            Label hotelName = new Label(1,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name"))));
            sheet.addCell(hotelName);
            Label address = new Label(2,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("addr"))));
            sheet.addCell(address);
            Label roomName = new Label(3,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name"))));
            sheet.addCell(roomName);
            Label mainten = new Label(4,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mainten_man"))));
            sheet.addCell(mainten);
            Label key = new Label(5,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("isKey"))));
            sheet.addCell(key);
            Label mac = new Label(6,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac"))));
            sheet.addCell(mac);
            Label count = new Label(7,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_days"))));
            sheet.addCell(count);
            Label time = new Label(8,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("production")))+"%");
            sheet.addCell(time);
            Label prod = new Label(9,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("av_production")))+"%");
            sheet.addCell(prod);
        }



        workbook.write();
        workbook.close();
    }

    @Test
    public void zhengwei() {
        long startTime = System.currentTimeMillis();
        String[] args = {
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-zhengwei",
//                "hbaseRoot=hdfs://onlinemain:8020/hbase",
//                "hbaseZookeeper=onlinemain",
//                "hdfsCluster=hdfs://onlinemain:8020",
//                "hbaseSharePath=/user/oozie/share/lib/lib_20170512162404/hbase",
                "hbaseRoot=hdfs://devpd1:8020/hbase",
                "hbaseZookeeper=devpd1",
                "hdfsCluster=hdfs://devpd1:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase",
                "startTime=2017073100",
                "endTime=2017073200"


        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {

            ToolRunner.run(conf, new Validate(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        System.out.println("totalTime=======>>>"+(endTime-startTime));
    }
    @Test
    public void test() throws IOException, WriteException {
        String[] args = {
                "hdfsCluster=hdfs://onlinemain:8020",
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
//                "hdfsOut=/home/data/hadoop/flume/text-mr/test-box_log",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain"

//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
//                "hdfsIn=/home/data/hadoop/flume/test-mr/mob_user",
//                "hdfsOut=/home/data/hadoop/flume/test-mr/test-mob_user",
//                "inRegex=^(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)$",
//                "table=user"
        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        Constant.CommonVariables.initMapReduce(conf, args);
        HBaseHelper hBaseHelper = new HBaseHelper(conf);
//        List<Result> boot_rate = hBaseHelper.getAllRecord("boot_rate");

        HTable table = new HTable(HBaseConfiguration.create(conf), "media_sta");
        Scan scan = new Scan();
        List<Filter> filters = new ArrayList<Filter>();
        SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                Bytes.toBytes("play_date"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes("20170905\t"));
        filters.add(timefilter);
        FilterList filterList = new FilterList(filters);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        List<Result> boot_rate = new ArrayList<>();
        for (Result result : scanner) {
            boot_rate.add(result);
        }
        scanner.close();

        for (Result result : boot_rate) {
            byte[] value = result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_date"));
            System.out.println(new String(value));
        }
    }

    @Test
    public void run3() throws IOException, WriteException {
        String[] args = {
                "hdfsCluster=hdfs://onlinemain:8020",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain"

        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();
        Constant.CommonVariables.initMapReduce(conf, args);
        HBaseHelper hBaseHelper = new HBaseHelper(conf);
        List<Result> boot_rate = hBaseHelper.getAllRecord("validate");


        WritableWorkbook workbook = Workbook.createWorkbook(new File("D:\\数据验证表格需求.xls"));

        WritableSheet sheet = workbook.createSheet("第一页", 0);

        Label area = new Label(0,0,"酒楼ID");
        sheet.addCell(area);
        Label hotel = new Label(1,0,"包间ID");
        sheet.addCell(hotel);
        Label addr = new Label(2,0,"播放日期");
        sheet.addCell(addr);
        Label server = new Label(3,0,"动作");
        sheet.addCell(server);
        Label maintenMan = new Label(4,0,"类型");
        sheet.addCell(maintenMan);
        Label iskey = new Label(5,0,"视频名称");
        sheet.addCell(iskey);
        Label boxMac = new Label(6,0,"视频长度");
        sheet.addCell(boxMac);
        Label playCount = new Label(7,0,"MAC");
        sheet.addCell(playCount);
        Label playTime = new Label(8,0,"播放时间");
        sheet.addCell(playTime);

        for (int i = 0; i < boot_rate.size(); i++) {
            Result result = boot_rate.get(i);
            Label areaName = new Label(0,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_id"))));
            sheet.addCell(areaName);
            Label hotelName = new Label(1,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_id"))));
            sheet.addCell(hotelName);
            Label address = new Label(2,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("date"))));
            sheet.addCell(address);
            Label roomName = new Label(3,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("time"))));
            sheet.addCell(roomName);
            Label mainten = new Label(4,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type"))));
            sheet.addCell(mainten);
            Label key = new Label(5,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("media_type"))));
            sheet.addCell(key);
            Label mac = new Label(6,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("content_name"))));
            sheet.addCell(mac);
            Label count = new Label(7,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_time"))));
            sheet.addCell(count);
            Label time = new Label(8,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac"))));
            sheet.addCell(time);
            Label prod = new Label(9,i+1,new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_date"))));
            sheet.addCell(prod);
        }



        workbook.write();
        workbook.close();
    }

}
