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
    private String areaId;

    @Setter
    @Getter
    private String areaName;

    @Setter
    @Getter
    private String hotelId;

    @Setter
    @Getter
    private String hotelName;

    @Setter
    @Getter
    private String roomId;

    @Setter
    @Getter
    private String roomName;

    @Setter
    @Getter
    private String boxId;

    @Setter
    @Getter
    private String boxName;

    @Setter
    @Getter
    private String mac;

    @Setter
    @Getter
    private String mediaId;

    @Setter
    @Getter
    private String mediaName;

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
        try {
            preparedStatement.setString(1,this.rowKey);
            preparedStatement.setString(2,this.areaId);
            preparedStatement.setString(3,this.areaName);
            preparedStatement.setString(4,this.hotelId);
            preparedStatement.setString(5,this.hotelName);
            preparedStatement.setString(6,this.roomId);
            preparedStatement.setString(7,this.roomName);
            preparedStatement.setString(8,this.boxId);
            preparedStatement.setString(9,this.boxName);
            preparedStatement.setString(10,this.mac);
            preparedStatement.setString(11,this.mediaId);
            preparedStatement.setString(12,this.mediaName);
            preparedStatement.setString(13,this.playTime);
            preparedStatement.setString(14,this.playCount);
            preparedStatement.setString(15,this.playDate);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        try {
            this.rowKey=resultSet.getString("row_key");
            this.areaId=resultSet.getString("area_id");
            this.areaName=resultSet.getString("area_name");
            this.hotelId=resultSet.getString("hotel_id");
            this.hotelName=resultSet.getString("hotel_name");
            this.roomId=resultSet.getString("room_id");
            this.roomName=resultSet.getString("room_name");
            this.boxId=resultSet.getString("box_id");
            this.boxName=resultSet.getString("box_name");
            this.mac=resultSet.getString("mac");
            this.mediaId=resultSet.getString("media_id");
            this.mediaName=resultSet.getString("media_name");
            this.playTime=resultSet.getString("play_time");
            this.playCount=resultSet.getString("play_count");
            this.playDate=resultSet.getString("play_date");
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
