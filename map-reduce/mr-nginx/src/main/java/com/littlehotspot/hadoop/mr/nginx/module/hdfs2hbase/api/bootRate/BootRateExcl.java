/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:31
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
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
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <h1>调度器 - 用户 [API]</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class BootRateExcl extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        try {
            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            String hbaseRoot = CommonVariables.getParameterValue(Argument.HbaseRoot);
            String hbaseZoo = CommonVariables.getParameterValue(Argument.HbaseZookeeper);
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
//
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);


            String startTime = CommonVariables.getParameterValue(Argument.StartTime);
            String endTime = CommonVariables.getParameterValue(Argument.EndTime);
            String excelName = CommonVariables.getParameterValue(Argument.ExcelName);

            HTable table = new HTable(HBaseConfiguration.create(this.getConf()), "boot_rate");
            Scan scan = new Scan();
            List<Filter> filters= new ArrayList<Filter>();
            SingleColumnValueFilter start = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("play_date"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(startTime));
            SingleColumnValueFilter end = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("play_date"), CompareFilter.CompareOp.LESS,Bytes.toBytes(endTime));
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


            WritableWorkbook workbook = Workbook.createWorkbook(new File(excelName));

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

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }
}
