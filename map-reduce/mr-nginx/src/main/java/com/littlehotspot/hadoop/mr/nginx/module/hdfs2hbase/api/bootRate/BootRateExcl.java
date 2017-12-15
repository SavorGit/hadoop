///**
// * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
// * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
// *
// * @Project : hadoop
// * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
// * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
// * @EMAIL 404644381@qq.com
// * @Time : 15:31
// */
//package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;
//
//import jxl.Workbook;
//import jxl.write.Label;
//import jxl.write.WritableSheet;
//import jxl.write.WritableWorkbook;
//import jxl.write.WriteException;
//import lombok.Getter;
//import lombok.NonNull;
//import lombok.RequiredArgsConstructor;
//import net.lizhaoweb.common.util.argument.ArgumentFactory;
//import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
//import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.ResultScanner;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.CompareFilter;
//import org.apache.hadoop.hbase.filter.Filter;
//import org.apache.hadoop.hbase.filter.FilterList;
//import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.IOException;
//import java.io.OutputStream;
//import java.net.URI;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * <h1>调度器 - 用户 [API]</h1>
// *
// * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
// * @version 1.0.0.0.1
// * @notes Created on 2017年06月01日<br>
// * Revision of last commit:$Revision$<br>
// * Author of last commit:$Author$<br>
// * Date of last commit:$Date$<br>
// */
//@RequiredArgsConstructor
//public class BootRateExcl {
//
//    @Getter
//    @NonNull
//    private Configuration conf;
//
//    public void run(String[] args) throws Exception {
//        MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);// 解析参数并初始化 MAP REDUCE
//
//        String hdfsCluster = ArgumentFactory.getParameterValue(Argument.HDFSCluster);
//
//        String startTime = ArgumentFactory.getParameterValue(BootRateArgument.StartTime);
//        ArgumentFactory.printInputArgument(BootRateArgument.StartTime, startTime, false);
//
//        String endTime = ArgumentFactory.getParameterValue(BootRateArgument.EndTime);
//        ArgumentFactory.printInputArgument(BootRateArgument.EndTime, endTime, false);
//
//        String excelName = ArgumentFactory.getParameterValue(BootRateArgument.ExcelName);
//        ArgumentFactory.printInputArgument(BootRateArgument.ExcelName, excelName, false);
//
//        String issue = ArgumentFactory.getParameterValue(BootRateArgument.Issue);
//        ArgumentFactory.printInputArgument(BootRateArgument.Issue, issue, false);
//
//        // 准备工作
//        ArgumentFactory.checkNullValueForArgument(BootRateArgument.HDFSCluster, hdfsCluster);
//        ArgumentFactory.checkNullValueForArgument(BootRateArgument.StartTime, startTime);
//        ArgumentFactory.checkNullValueForArgument(BootRateArgument.EndTime, endTime);
//        ArgumentFactory.checkNullValueForArgument(BootRateArgument.Issue, issue);
//        ArgumentFactory.checkNullValueForArgument(BootRateArgument.ExcelName, excelName);
//
//        URI uri = new URI(hdfsCluster);
//        FileSystem fs = FileSystem.get(uri, this.getConf());
//        Path outputPath = new Path(excelName);
//        OutputStream outputStream = fs.create(outputPath, true);
//
//        WritableWorkbook workbook = Workbook.createWorkbook(outputStream);
////        WritableWorkbook workbook = Workbook.createWorkbook(new File(excelName));
//        this.exportBootRateDetails(workbook, startTime, endTime);// 开机率明细
//        this.exportBootRateSummary(workbook, issue);// 开机率汇总
//
//        workbook.write();
//        workbook.close();
//    }
//
//    // 开机率明细
//    private void exportBootRateDetails(WritableWorkbook workbook, String startTime, String endTime) throws IOException, WriteException {
//        HTable table = new HTable(HBaseConfiguration.create(this.getConf()), "boot_rate");
//
//        Scan scan = new Scan();
//        List<Filter> filters = new ArrayList<>();
//        SingleColumnValueFilter start = new SingleColumnValueFilter(Bytes.toBytes("attr"), Bytes.toBytes("play_date"), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(startTime));
//        SingleColumnValueFilter end = new SingleColumnValueFilter(Bytes.toBytes("attr"), Bytes.toBytes("play_date"), CompareFilter.CompareOp.LESS, Bytes.toBytes(endTime));
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
//
//        WritableSheet sheet = workbook.createSheet("开机率明细", 0);
//
//        Label area = new Label(0, 0, "区域");
//        sheet.addCell(area);
//        Label hotel = new Label(1, 0, "酒楼");
//        sheet.addCell(hotel);
//        Label addr = new Label(2, 0, "位置");
//        sheet.addCell(addr);
//        Label server = new Label(3, 0, "包间");
//        sheet.addCell(server);
//        Label maintenMan = new Label(4, 0, "维护人");
//        sheet.addCell(maintenMan);
//        Label iskey = new Label(5, 0, "重点酒楼");
//        sheet.addCell(iskey);
//        Label playDate = new Label(6, 0, "播放日期");
//        sheet.addCell(playDate);
//        Label boxMac = new Label(7, 0, "机顶盒编号");
//        sheet.addCell(boxMac);
//        Label playCount = new Label(8, 0, "播放次数");
//        sheet.addCell(playCount);
//        Label playTime = new Label(9, 0, "播放总秒数");
//        sheet.addCell(playTime);
//        Label production = new Label(10, 0, "开机率");
//        sheet.addCell(production);
//        for (int i = 0; i < boot_rate.size(); i++) {
//            Result result = boot_rate.get(i);
//            Label areaName = new Label(0, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area"))));
//            sheet.addCell(areaName);
//            Label hotelName = new Label(1, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name"))));
//            sheet.addCell(hotelName);
//            Label address = new Label(2, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("addr"))));
//            sheet.addCell(address);
//            Label roomName = new Label(3, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name"))));
//            sheet.addCell(roomName);
//            Label mainten = new Label(4, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mainten_man"))));
//            sheet.addCell(mainten);
//            Label key = new Label(5, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("isKey"))));
//            sheet.addCell(key);
//            Label date = new Label(6, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_date"))));
//            sheet.addCell(date);
//            Label mac = new Label(7, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac"))));
//            sheet.addCell(mac);
//            Label count = new Label(8, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_count"))));
//            sheet.addCell(count);
//            Label time = new Label(9, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_time"))));
//            sheet.addCell(time);
//            Label prod = new Label(10, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("production"))));
//            sheet.addCell(prod);
//        }
//    }
//
//    // 开机率汇总
//    private void exportBootRateSummary(WritableWorkbook workbook, String issue) throws IOException, WriteException {
//        HTable table = new HTable(HBaseConfiguration.create(this.getConf()), "total_boot_rate");
//        Scan scan = new Scan();
//        List<Filter> filters = new ArrayList<>();
//        SingleColumnValueFilter issuefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"), Bytes.toBytes("issue"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(issue));
//        filters.add(issuefilter);
//
//        FilterList filterList = new FilterList(filters);
//        scan.setFilter(filterList);
//        ResultScanner scanner = table.getScanner(scan);
//        List<Result> boot_rate1 = new ArrayList<>();
//        for (Result result : scanner) {
//            boot_rate1.add(result);
//        }
//        scanner.close();
//
//        WritableSheet sheet = workbook.createSheet("开机率汇总", 1);
//
//        Label area1 = new Label(0, 0, "区域");
//        sheet.addCell(area1);
//        Label hotel1 = new Label(1, 0, "酒楼");
//        sheet.addCell(hotel1);
//        Label addr1 = new Label(2, 0, "位置");
//        sheet.addCell(addr1);
//        Label server1 = new Label(3, 0, "包间");
//        sheet.addCell(server1);
//        Label maintenMan1 = new Label(4, 0, "维护人");
//        sheet.addCell(maintenMan1);
//        Label iskey1 = new Label(5, 0, "重点酒楼");
//        sheet.addCell(iskey1);
//        Label boxMac1 = new Label(6, 0, "机顶盒编号");
//        sheet.addCell(boxMac1);
//        Label playCount1 = new Label(7, 0, "播放天数");
//        sheet.addCell(playCount1);
//        Label playTime1 = new Label(8, 0, "有效率合计");
//        sheet.addCell(playTime1);
//        Label production1 = new Label(9, 0, "平均有效率");
//        sheet.addCell(production1);
//
//        for (int i = 0; i < boot_rate1.size(); i++) {
//            Result result = boot_rate1.get(i);
//            Label areaName = new Label(0, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area"))));
//            sheet.addCell(areaName);
//            Label hotelName = new Label(1, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name"))));
//            sheet.addCell(hotelName);
//            Label address = new Label(2, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("addr"))));
//            sheet.addCell(address);
//            Label roomName = new Label(3, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name"))));
//            sheet.addCell(roomName);
//            Label mainten = new Label(4, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mainten_man"))));
//            sheet.addCell(mainten);
//            Label key = new Label(5, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("isKey"))));
//            sheet.addCell(key);
//            Label mac = new Label(6, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac"))));
//            sheet.addCell(mac);
//            Label count = new Label(7, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_days"))));
//            sheet.addCell(count);
//            Label time = new Label(8, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("production"))) + "%");
//            sheet.addCell(time);
//            Label prod = new Label(9, i + 1, new String(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("av_production"))) + "%");
//            sheet.addCell(prod);
//        }
//    }
//}
