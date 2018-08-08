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
    private int areaId;

    @Setter
    @Getter
    private String areaName;

    @Setter
    @Getter
    private long hotelId;

    @Setter
    @Getter
    private String hotelName;

    @Setter
    @Getter
    private long roomId;

    @Setter
    @Getter
    private String roomName;

    @Setter
    @Getter
    private long boxId;

    @Setter
    @Getter
    private String boxName;

    @Setter
    @Getter
    private String mac;

    @Setter
    @Getter
    private long mediaId;

    @Setter
    @Getter
    private String mediaName;

    @Setter
    @Getter
    private int playTime;

    @Setter
    @Getter
    private int playCount;

    @Setter
    @Getter
    private int playDate;


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        try {
            preparedStatement.setString(1, this.rowKey);
            preparedStatement.setInt(2, this.areaId);
            preparedStatement.setString(3, this.areaName);
            preparedStatement.setLong(4, this.hotelId);
            preparedStatement.setString(5, this.hotelName);
            preparedStatement.setLong(6, this.roomId);
            preparedStatement.setString(7, this.roomName);
            preparedStatement.setLong(8, this.boxId);
            preparedStatement.setString(9, this.boxName);
            preparedStatement.setString(10, this.mac);
            preparedStatement.setLong(11, this.mediaId);
            preparedStatement.setString(12, this.mediaName);
            preparedStatement.setInt(13, this.playCount);
            preparedStatement.setInt(14, this.playTime);
            preparedStatement.setInt(15, this.playDate);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        try {
            this.rowKey = resultSet.getString("row_key");
            this.areaId = resultSet.getInt("area_id");
            this.areaName = resultSet.getString("area_name");
            this.hotelId = resultSet.getLong("hotel_id");
            this.hotelName = resultSet.getString("hotel_name");
            this.roomId = resultSet.getLong("room_id");
            this.roomName = resultSet.getString("room_name");
            this.boxId = resultSet.getLong("box_id");
            this.boxName = resultSet.getString("box_name");
            this.mac = resultSet.getString("mac");
            this.mediaId = resultSet.getLong("media_id");
            this.mediaName = resultSet.getString("media_name");
            this.playTime = resultSet.getInt("play_time");
            this.playCount = resultSet.getInt("play_count");
            this.playDate = resultSet.getInt("play_date");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
