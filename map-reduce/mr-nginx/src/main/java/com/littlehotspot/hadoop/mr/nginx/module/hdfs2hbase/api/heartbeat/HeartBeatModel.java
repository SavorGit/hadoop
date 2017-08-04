package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.heartbeat;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Created by gy on 2017/8/2.
 */
@NoArgsConstructor
public class HeartBeatModel implements Writable, DBWritable {

    private static final char FIELD_DELIMITER = 0x0001;

    @Setter
    @Getter
    private long boxId;

    @Setter
    @Getter
    private String boxMac;

    @Setter
    @Getter
    private String boxName;

    @Setter
    @Getter
    private long roomId;

    @Setter
    @Getter
    private String roomName;

    @Setter
    @Getter
    private long hotelId;

    @Setter
    @Getter
    private String hotelName;

    @Setter
    @Getter
    private long areaId;

    @Setter
    @Getter
    private String areaName;

    @Setter
    @Getter
    private Timestamp lastHeartTime;

    @Setter
    @Getter
    private int type;

    @Setter
    @Getter
    private String hotelIp;

    @Setter
    @Getter
    private String smallIp;

    @Setter
    @Getter
    private String adsPeriod;

    @Setter
    @Getter
    private String demandPeriod;

    @Setter
    @Getter
    private String apkVersion;

    @Setter
    @Getter
    private String warVersion;

    @Setter
    @Getter
    private String logoPeriod;

    public String toString() {
        StringBuffer toString = new StringBuffer();
        toString.append(this.boxId).append(FIELD_DELIMITER);
        toString.append(this.boxMac).append(FIELD_DELIMITER);
        toString.append(this.boxName).append(FIELD_DELIMITER);
        toString.append(this.roomId).append(FIELD_DELIMITER);
        toString.append(this.roomName).append(FIELD_DELIMITER);
        toString.append(this.hotelId).append(FIELD_DELIMITER);
        toString.append(this.hotelName).append(FIELD_DELIMITER);
        toString.append(this.areaId).append(FIELD_DELIMITER);
        toString.append(this.areaName).append(FIELD_DELIMITER);
        toString.append(this.lastHeartTime).append(FIELD_DELIMITER);
        toString.append(this.type).append(FIELD_DELIMITER);
        toString.append(this.hotelIp).append(FIELD_DELIMITER);
        toString.append(this.smallIp).append(FIELD_DELIMITER);
        toString.append(this.adsPeriod).append(FIELD_DELIMITER);
        toString.append(this.demandPeriod).append(FIELD_DELIMITER);
        toString.append(this.apkVersion).append(FIELD_DELIMITER);
        toString.append(this.warVersion).append(FIELD_DELIMITER);
        toString.append(this.logoPeriod);
        return toString.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setLong(1,this.boxId);
        preparedStatement.setString(2,this.boxMac);
        preparedStatement.setString(3,this.boxName);
        preparedStatement.setLong(4,this.roomId);
        preparedStatement.setString(5,this.roomName);
        preparedStatement.setLong(6,this.hotelId);
        preparedStatement.setString(7,this.hotelName);
        preparedStatement.setLong(8,this.areaId);
        preparedStatement.setString(9,this.areaName);
        preparedStatement.setTimestamp(10,this.lastHeartTime);
        preparedStatement.setInt(11,this.type);
        preparedStatement.setString(12,this.hotelIp);
        preparedStatement.setString(13,this.smallIp);
        preparedStatement.setString(14,this.adsPeriod);
        preparedStatement.setString(15,this.demandPeriod);
        preparedStatement.setString(16,this.apkVersion);
        preparedStatement.setString(17,this.warVersion);
        preparedStatement.setString(18,this.logoPeriod);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.boxId=resultSet.getLong("box_id");
        this.boxMac=resultSet.getString("box_mac");
        this.boxName=resultSet.getString("box_name");
        this.roomId=resultSet.getLong("room_id");
        this.roomName=resultSet.getString("room_name");
        this.hotelId=resultSet.getLong("hotel_id");
        this.hotelName=resultSet.getString("hotel_name");
        this.areaId=resultSet.getLong("area_id");
        this.areaName=resultSet.getString("area_name");
        this.lastHeartTime=resultSet.getTimestamp("last_heart_time");
        this.type=resultSet.getInt("type");
        this.hotelIp=resultSet.getString("hotel_ip");
        this.smallIp=resultSet.getString("small_ip");
        this.adsPeriod=resultSet.getString("ads_period");
        this.demandPeriod=resultSet.getString("demand_period");
        this.apkVersion=resultSet.getString("apk_version");
        this.warVersion=resultSet.getString("war_version");
        this.logoPeriod=resultSet.getString("logo_period");
    }
}
