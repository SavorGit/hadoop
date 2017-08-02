/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.hbase.io
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:49
 */
package com.littlehotspot.hadoop.mr.hbase.io;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2017年08月02日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class HotelWritable implements Writable, DBWritable {

    private static final char FIELD_DELIMITER = 0x0001;


    @Setter
    @Getter
    private Long id;// 酒楼信息表id
    private String name;// 酒楼名称
    private String address;// 酒楼地址
    private Long areaId;// 区域id
    private Integer mediaId;// 资源id
    private String contractor;// 酒楼联系人
    private String mobile;// 手机
    private String tel;// 联系电话
    private String maintainer;// 合作维护人
    private String level;// 酒楼级别
    private Integer isKey;// 重点酒楼 1--> 重点  2 -->非重点
    private long installDate;// 安装日期
    private Integer state;// 酒楼状态 1正常 2冻结 3报损
    private Integer stateChangeReason;// 状态变更说明 1正常   2倒闭   3装修    4淘汰    5放假    6易主    7终止合作    8问题沟通中
    private String gps;//
    private String remark;// 备注
    private Integer boxType;// 酒楼机顶盒类型1:一代单机版2:二代网络版3:三代5G版
    private long createTime;//
    private long updateTime;//
    private Integer flag;// 状态标识：0正常，1删除
    private String techMaintainer;// 技术运维人
    private String remoteId;// 远程ID
    private String hotelWifi;// 酒楼wifi名称
    private String hotelWifiPassword;// 酒楼wifi密码
    private String billPer;// 对账单联系人
    private String billTel;// 对账单人联系电话
    private String collectionCompany;// 收款公司名称
    private String bankAccount;// 银行账号
    private String bankName;// 开户行名称

    private String macAddress;// mac地址
    private String ipLocal;// 小平台内网ip
    private String ip;// 外网ip
    private String serverLocation;// 服务器位置

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setString(2, this.name);
        statement.setString(3, this.address);
        statement.setLong(4, this.areaId);
        statement.setInt(5, this.mediaId);
        statement.setString(6, this.contractor);
        statement.setString(7, this.mobile);
        statement.setString(8, this.tel);
        statement.setString(9, this.maintainer);
        statement.setString(10, this.level);
        statement.setInt(11, this.isKey);
        statement.setLong(12, this.installDate);
        statement.setInt(13, this.state);
        statement.setInt(14, this.stateChangeReason);
        statement.setString(15, this.gps);
        statement.setString(16, this.remark);
        statement.setInt(17, this.boxType);
        statement.setLong(18, this.createTime);
        statement.setLong(19, this.updateTime);
        statement.setInt(20, this.flag);
        statement.setString(21, this.techMaintainer);
        statement.setString(22, this.remoteId);
        statement.setString(23, this.hotelWifi);
        statement.setString(24, this.hotelWifiPassword);
        statement.setString(25, this.billPer);
        statement.setString(26, this.billTel);
        statement.setString(27, this.collectionCompany);
        statement.setString(28, this.bankAccount);
        statement.setString(29, this.bankName);
        statement.setString(30, this.macAddress);
        statement.setString(31, this.ipLocal);
        statement.setString(32, this.ip);
        statement.setString(33, this.serverLocation);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.id = result.getLong("id");
        this.name = result.getString("name");
        this.address = result.getString("address");
        this.areaId = result.getLong("areaId");
        this.mediaId = result.getInt("mediaId");
        this.contractor = result.getString("contractor");
        this.mobile = result.getString("mobile");
        this.tel = result.getString("tel");
        this.maintainer = result.getString("maintainer");
        this.level = result.getString("level");
        this.isKey = result.getInt("isKey");
        this.installDate = result.getTimestamp("installDate").getTime();
        this.state = result.getInt("state");
        this.stateChangeReason = result.getInt("stateChangeReason");
        this.gps = result.getString("gps");
        this.remark = result.getString("remark");
        this.boxType = result.getInt("boxType");
        this.createTime = result.getTimestamp("createTime").getTime();
        this.updateTime = result.getTimestamp("updateTime").getTime();
        this.flag = result.getInt("flag");
        this.techMaintainer = result.getString("techMaintainer");
        this.remoteId = result.getString("remoteId");
        this.hotelWifi = result.getString("hotelWifi");
        this.hotelWifiPassword = result.getString("hotelWifiPassword");
        this.billPer = result.getString("billPer");
        this.billTel = result.getString("billTel");
        this.collectionCompany = result.getString("collectionCompany");
        this.bankAccount = result.getString("bankAccount");
        this.bankName = result.getString("bankName");
        this.macAddress = result.getString("macAddress");
        this.ipLocal = result.getString("ipLocal");
        this.ip = result.getString("ip");
        this.serverLocation = result.getString("serverLocation");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.id);
        Text.writeString(dataOutput, this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readLong();
        String line = Text.readString(dataInput);
        this.buildBean(line);
    }

    @Override
    public String toString() {
        StringBuilder toString;
        toString = new StringBuilder();
        toString.append(this.id).append(FIELD_DELIMITER);
        toString.append(this.name).append(FIELD_DELIMITER);
        toString.append(this.address).append(FIELD_DELIMITER);
        toString.append(this.areaId).append(FIELD_DELIMITER);
        toString.append(this.mediaId).append(FIELD_DELIMITER);
        toString.append(this.contractor).append(FIELD_DELIMITER);
        toString.append(this.mobile).append(FIELD_DELIMITER);
        toString.append(this.tel).append(FIELD_DELIMITER);
        toString.append(this.maintainer).append(FIELD_DELIMITER);
        toString.append(this.level).append(FIELD_DELIMITER);
        toString.append(this.isKey).append(FIELD_DELIMITER);
        toString.append(this.installDate).append(FIELD_DELIMITER);
        toString.append(this.state).append(FIELD_DELIMITER);
        toString.append(this.stateChangeReason).append(FIELD_DELIMITER);
        toString.append(this.gps).append(FIELD_DELIMITER);
        toString.append(this.remark).append(FIELD_DELIMITER);
        toString.append(this.boxType).append(FIELD_DELIMITER);
        toString.append(this.createTime).append(FIELD_DELIMITER);
        toString.append(this.updateTime).append(FIELD_DELIMITER);
        toString.append(this.flag).append(FIELD_DELIMITER);
        toString.append(this.techMaintainer).append(FIELD_DELIMITER);
        toString.append(this.remoteId).append(FIELD_DELIMITER);
        toString.append(this.hotelWifi).append(FIELD_DELIMITER);
        toString.append(this.hotelWifiPassword).append(FIELD_DELIMITER);
        toString.append(this.billPer).append(FIELD_DELIMITER);
        toString.append(this.billTel).append(FIELD_DELIMITER);
        toString.append(this.collectionCompany).append(FIELD_DELIMITER);
        toString.append(this.bankAccount).append(FIELD_DELIMITER);
        toString.append(this.bankName).append(FIELD_DELIMITER);
        toString.append(this.macAddress).append(FIELD_DELIMITER);
        toString.append(this.ipLocal).append(FIELD_DELIMITER);
        toString.append(this.ip).append(FIELD_DELIMITER);
        toString.append(this.serverLocation);
        return super.toString();
    }

    public Put toPut() {
        if (this.id == null) {
            throw new IllegalStateException("The id of hotel-bean for function[toPut]");
        }
        String familyName;
        long version = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(this.id));// 设置rowkey

        // 基本属性
        familyName = "attr";
        if (this.id != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("id"), version, Bytes.toBytes(this.id));
        }
        if (this.name != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("name"), version, Bytes.toBytes(this.name));
        }
        if (this.address != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("addr"), version, Bytes.toBytes(this.address));
        }
        if (this.areaId != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("area_id"), version, Bytes.toBytes(this.areaId));
        }
        if (this.mediaId != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("media_id"), version, Bytes.toBytes(this.mediaId));
        }
        if (this.contractor != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("contractor"), version, Bytes.toBytes(this.contractor));
        }
        if (this.mobile != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mobile"), version, Bytes.toBytes(this.mobile));
        }
        if (this.tel != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("tel"), version, Bytes.toBytes(this.tel));
        }
        if (this.maintainer != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("maintainer"), version, Bytes.toBytes(this.maintainer));
        }
        if (this.level != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("level"), version, Bytes.toBytes(this.level));
        }
        if (this.isKey != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("iskey"), version, Bytes.toBytes(this.isKey));
        }
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("install_date"), version, Bytes.toBytes(this.installDate));
        if (this.state != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("state"), version, Bytes.toBytes(this.state));
        }
        if (this.stateChangeReason != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("state_change_reason"), version, Bytes.toBytes(this.stateChangeReason));
        }
        if (this.gps != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("gps"), version, Bytes.toBytes(this.gps));
        }
        if (this.remark != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("remark"), version, Bytes.toBytes(this.remark));
        }
        if (this.boxType != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_box_type"), version, Bytes.toBytes(this.boxType));
        }
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("create_time"), version, Bytes.toBytes(this.createTime));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("update_time"), version, Bytes.toBytes(this.updateTime));
        if (this.flag != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("flag"), version, Bytes.toBytes(this.flag));
        }
        if (this.techMaintainer != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("tech_maintainer"), version, Bytes.toBytes(this.techMaintainer));
        }
        if (this.remoteId != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("remote_id"), version, Bytes.toBytes(this.remoteId));
        }
        if (this.hotelWifi != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_wifi"), version, Bytes.toBytes(this.hotelWifi));
        }
        if (this.hotelWifiPassword != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("hotel_wifi_pas"), version, Bytes.toBytes(this.hotelWifiPassword));
        }
        if (this.billPer != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("bill_per"), version, Bytes.toBytes(this.billPer));
        }
        if (this.billTel != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("bill_tel"), version, Bytes.toBytes(this.billTel));
        }
        if (this.collectionCompany != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("collection_company"), version, Bytes.toBytes(this.collectionCompany));
        }
        if (this.bankAccount != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("bank_account"), version, Bytes.toBytes(this.bankAccount));
        }
        if (this.bankName != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("bank_name"), version, Bytes.toBytes(this.bankName));
        }

        // 扩展属性
        familyName = "ext";
        if (this.macAddress != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("mac_addr"), version, Bytes.toBytes(this.macAddress));
        }
        if (this.ipLocal != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ip_local"), version, Bytes.toBytes(this.ipLocal));
        }
        if (this.ip != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("ip"), version, Bytes.toBytes(this.ip));
        }
        if (this.serverLocation != null) {
            put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("server_location"), version, Bytes.toBytes(this.serverLocation));
        }

        return put;
    }

    private void buildBean(String line) {
    }
}
