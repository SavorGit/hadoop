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
public class HotelWritable extends AbstractWritable implements Writable, DBWritable {

    // 基本属性
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

    // 扩展属性
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
        // 基本属性
        this.setValue(HotelWritable.class, this, "id", result, "id");
        this.setValue(HotelWritable.class, this, "name", result, "name");
        this.setValue(HotelWritable.class, this, "address", result, "address");
        this.setValue(HotelWritable.class, this, "areaId", result, "areaId");
        this.setValue(HotelWritable.class, this, "mediaId", result, "mediaId");
        this.setValue(HotelWritable.class, this, "contractor", result, "contractor");
        this.setValue(HotelWritable.class, this, "mobile", result, "mobile");
        this.setValue(HotelWritable.class, this, "tel", result, "tel");
        this.setValue(HotelWritable.class, this, "maintainer", result, "maintainer");
        this.setValue(HotelWritable.class, this, "level", result, "level");
        this.setValue(HotelWritable.class, this, "isKey", result, "isKey");
        this.setValue(HotelWritable.class, this, "installDate", result, "installDate");
        this.setValue(HotelWritable.class, this, "state", result, "state");
        this.setValue(HotelWritable.class, this, "stateChangeReason", result, "stateChangeReason");
        this.setValue(HotelWritable.class, this, "gps", result, "gps");
        this.setValue(HotelWritable.class, this, "remark", result, "remark");
        this.setValue(HotelWritable.class, this, "boxType", result, "boxType");
        this.setValue(HotelWritable.class, this, "createTime", result, "createTime");
        this.setValue(HotelWritable.class, this, "updateTime", result, "updateTime");
        this.setValue(HotelWritable.class, this, "flag", result, "flag");
        this.setValue(HotelWritable.class, this, "techMaintainer", result, "techMaintainer");
        this.setValue(HotelWritable.class, this, "remoteId", result, "remoteId");
        this.setValue(HotelWritable.class, this, "hotelWifi", result, "hotelWifi");
        this.setValue(HotelWritable.class, this, "hotelWifiPassword", result, "hotelWifiPassword");
        this.setValue(HotelWritable.class, this, "billPer", result, "billPer");
        this.setValue(HotelWritable.class, this, "billTel", result, "billTel");
        this.setValue(HotelWritable.class, this, "collectionCompany", result, "collectionCompany");
        this.setValue(HotelWritable.class, this, "bankAccount", result, "bankAccount");
        this.setValue(HotelWritable.class, this, "bankName", result, "bankName");

        // 扩展属性
        this.setValue(HotelWritable.class, this, "macAddress", result, "macAddress");
        this.setValue(HotelWritable.class, this, "ipLocal", result, "ipLocal");
        this.setValue(HotelWritable.class, this, "ip", result, "ip");
        this.setValue(HotelWritable.class, this, "serverLocation", result, "serverLocation");
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
        Put put = new Put(Bytes.toBytes(this.id+""));// 设置rowkey

        // 基本属性
        familyName = "attr";
        this.addColumn(HotelWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(HotelWritable.class, this, "name", put, familyName, "name", version);
        this.addColumn(HotelWritable.class, this, "address", put, familyName, "addr", version);
        this.addColumn(HotelWritable.class, this, "areaId", put, familyName, "area_id", version);
        this.addColumn(HotelWritable.class, this, "mediaId", put, familyName, "media_id", version);
        this.addColumn(HotelWritable.class, this, "contractor", put, familyName, "contractor", version);
        this.addColumn(HotelWritable.class, this, "mobile", put, familyName, "mobile", version);
        this.addColumn(HotelWritable.class, this, "tel", put, familyName, "tel", version);
        this.addColumn(HotelWritable.class, this, "maintainer", put, familyName, "maintainer", version);
        this.addColumn(HotelWritable.class, this, "level", put, familyName, "level", version);
        this.addColumn(HotelWritable.class, this, "isKey", put, familyName, "iskey", version);
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("install_date"), version, Bytes.toBytes(this.installDate+""));
        this.addColumn(HotelWritable.class, this, "state", put, familyName, "state", version);
        this.addColumn(HotelWritable.class, this, "stateChangeReason", put, familyName, "state_change_reason", version);
        this.addColumn(HotelWritable.class, this, "gps", put, familyName, "gps", version);
        this.addColumn(HotelWritable.class, this, "remark", put, familyName, "remark", version);
        this.addColumn(HotelWritable.class, this, "boxType", put, familyName, "hotel_box_type", version);
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("create_time"), version, Bytes.toBytes(this.createTime+""));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("update_time"), version, Bytes.toBytes(this.updateTime+""));
        this.addColumn(HotelWritable.class, this, "flag", put, familyName, "flag", version);
        this.addColumn(HotelWritable.class, this, "techMaintainer", put, familyName, "tech_maintainer", version);
        this.addColumn(HotelWritable.class, this, "remoteId", put, familyName, "remote_id", version);
        this.addColumn(HotelWritable.class, this, "hotelWifi", put, familyName, "hotel_wifi", version);
        this.addColumn(HotelWritable.class, this, "hotelWifiPassword", put, familyName, "hotel_wifi_pas", version);
        this.addColumn(HotelWritable.class, this, "billPer", put, familyName, "bill_per", version);
        this.addColumn(HotelWritable.class, this, "billTel", put, familyName, "bill_tel", version);
        this.addColumn(HotelWritable.class, this, "collectionCompany", put, familyName, "collection_company", version);
        this.addColumn(HotelWritable.class, this, "bankAccount", put, familyName, "bank_account", version);
        this.addColumn(HotelWritable.class, this, "bankName", put, familyName, "bank_name", version);

        // 扩展属性
        familyName = "ext";
        this.addColumn(HotelWritable.class, this, "macAddress", put, familyName, "mac_addr", version);
        this.addColumn(HotelWritable.class, this, "ipLocal", put, familyName, "ip_local", version);
        this.addColumn(HotelWritable.class, this, "ip", put, familyName, "ip", version);
        this.addColumn(HotelWritable.class, this, "serverLocation", put, familyName, "server_location", version);

        return put;
    }


    private void buildBean(String line) {
    }
}
