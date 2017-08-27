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
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
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
        List<Result> boot_rate = hBaseHelper.getAllRecord("boot_rate");

        WritableWorkbook workbook = Workbook.createWorkbook(new File("D:\\boot_rate.xls"));

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
        String[] args = {
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-root_rate",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain",
                "hdfsCluster=hdfs://onlinemain:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170512162404/hbase",
                "startTime=2017080100",
                "endTime=2017081600"


        };
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.3");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new BoxCleanJob(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void total() {
        String[] args = {
//                "hdfsIn=/home/data/hadoop/flume/nginx_log/export/2017-05-31",
                "hdfsOut=/home/data/hadoop/flume/text-mr/test-total_root_rate",
                "hbaseRoot=hdfs://onlinemain:8020/hbase",
                "hbaseZookeeper=onlinemain",
                "hdfsCluster=hdfs://onlinemain:8020",
                "hbaseSharePath=/user/oozie/share/lib/lib_20170512162404/hbase"


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

        WritableWorkbook workbook = Workbook.createWorkbook(new File("D:\\total_boot_rate.xls"));

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

}
