/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hbase.hotel
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 09:23
 */
package com.littlehotspot.hadoop.mr.hbase.hotel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Locale;

import static org.fusesource.jansi.Ansi.Color.YELLOW;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年08月03日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class TestMysql2HBaseScheduler {

    private static final String FORMAT_PRINT_TIME_CONSUMING = "执行 %s 用时 %s 毫秒";
    private static final String FORMAT_COLOR_PRINT_TIME_CONSUMING = "@|blue 执行|@ @|green %s|@ @|blue 用时|@ @|red %s|@ @|blue 毫秒|@\n";

    private DecimalFormat decimalFormat;

    private Configuration conf;

    @Before
    public void init() throws IOException {
        System.out.println();
        System.setProperty("hadoop.home.dir", "D:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        Locale.setDefault(Locale.CHINA);
        this.decimalFormat = new DecimalFormat();

        this.conf = new Configuration();
//        this.conf.set("mapred.job.tracker", "local");
//        this.conf.set("mapreduce.framework.name", "yarn");
//        this.conf.set("yarn.resoucemanager.hostname", "localhost");
//        this.conf.set("yarn.resourcemanager.address", "localhost:8032");
//        this.conf.set("yarn.resourcemanager.scheduler.address", "localhost:8030");
//        this.conf.set("yarn.resourcemanager.resource-tracker.address", "localhost:8035");
//        this.conf.set("yarn.resourcemanager.admin.address", "localhost:8033");
    }

    @Test
    public void hotelMysql2HBase() {
        long start = System.currentTimeMillis();
        String[] args = {
                "jobName=Import data to hBase from mysql for hotel",
                "hdfsCluster=hdfs://localhost:9000",
                "hdfsOut=/home/data/hadoop/flume/mysql/medias-hbase",
                "jdbcDriver=com.mysql.jdbc.Driver",
                "jdbcUrl=jdbc:mysql://192.168.2.145:3306/cloud",
                "jdbcUsername=javaweb",
                "jdbcPassword=123456",
                "hBaseTable=hotel",
                "jdbcSql=SELECT hotel.id AS id, hotel.name AS name, hotel.addr AS address, hotel.area_id AS areaId, hotel.media_id AS mediaId, hotel.contractor AS contractor, hotel.mobile AS mobile, hotel.tel AS tel, hotel.maintainer AS maintainer, hotel.level AS level, hotel.iskey AS isKey, hotel.install_date AS installDate, hotel.state AS state, hotel.state_change_reason AS stateChangeReason, hotel.gps AS gps, hotel.remark AS remark, hotel.hotel_box_type AS boxType, hotel.create_time AS createTime, hotel.update_time AS updateTime, hotel.flag AS flag, hotel.tech_maintainer AS techMaintainer, hotel.remote_id AS remoteId, hotel.hotel_wifi AS hotelWifi, hotel.hotel_wifi_pas AS hotelWifiPassword, hotel.bill_per AS billPer, hotel.bill_tel AS billTel, hotel.collection_company AS collectionCompany, hotel.bank_account AS bankAccount, hotel.bank_name AS bankName, ext.mac_addr AS macAddress, ext.ip_local AS ipLocal, ext.ip AS ip, ext.server_location AS serverLocation FROM savor_hotel AS hotel LEFT JOIN savor_hotel_ext AS ext ON hotel.id=ext.hotel_id ORDER BY id ASC",
                "writableClass=com.littlehotspot.hadoop.mr.hbase.io.HotelWritable"
        };

        try {
            ToolRunner.run(this.conf, new Mysql2HBaseScheduler(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();

        String printMessage = String.format(FORMAT_PRINT_TIME_CONSUMING, Thread.currentThread().getStackTrace()[1].getMethodName(), this.decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().fg(YELLOW).a(printMessage).reset());

        String ansiPrintMessage = String.format(FORMAT_COLOR_PRINT_TIME_CONSUMING, Thread.currentThread().getStackTrace()[1].getMethodName(), this.decimalFormat.format(end - start));
        System.out.println(ansi().eraseScreen().render(ansiPrintMessage));
    }
}
