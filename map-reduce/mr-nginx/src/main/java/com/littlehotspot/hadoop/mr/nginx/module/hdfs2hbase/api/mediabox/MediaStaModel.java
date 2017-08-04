package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.mediabox;

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

/**
 * Created by gy on 2017/8/2.
 */
@NoArgsConstructor
public class MediaStaModel implements Writable, DBWritable {

    @Setter
    @Getter
    private String rowKey;

    @Setter
    @Getter
    private String areaName;

    @Setter
    @Getter
    private String hotelName;

    @Setter
    @Getter
    private String roomName;

    @Setter
    @Getter
    private String mac;

    @Setter
    @Getter
    private String tvCount;

    @Setter
    @Getter
    private String playTime;

    @Setter
    @Getter
    private String playCount;

    @Setter
    @Getter
    private String playDate;



    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,this.rowKey);
        preparedStatement.setString(2,this.areaName);
        preparedStatement.setString(3,this.hotelName);
        preparedStatement.setString(4,this.roomName);
        preparedStatement.setString(5,this.mac);
        preparedStatement.setString(6,this.tvCount);
        preparedStatement.setString(7,this.playTime);
        preparedStatement.setString(8,this.playCount);
        preparedStatement.setString(9,this.playDate);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.rowKey=resultSet.getString("row_key");
        this.areaName=resultSet.getString("area_name");
        this.hotelName=resultSet.getString("hotel_name");
        this.roomName=resultSet.getString("room_name");
        this.mac=resultSet.getString("mac");
        this.tvCount=resultSet.getString("tv_count");
        this.playTime=resultSet.getString("play_time");
        this.playCount=resultSet.getString("play_count");
        this.playDate=resultSet.getString("play_date");
    }
}
