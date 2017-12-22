package com.littlehotspot.hadoop.mr.probe.model;

import lombok.Data;
import lombok.NoArgsConstructor;
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
@Data
public class ProbeModel implements Writable, DBWritable {
    /**
     * 探针mac
     */
    private String probeMac;

    /**
     * 手机mac
     */
    private String mobileMac;

    /**
     * 信号强度
     */
    private String signalInten;

    /**
     * wifi类型
     */
    private String wifiType;

    /**
     * 上报日期
     */
    private String timeStamp;

    /**
     * 酒店Id
     */
    private String hotelId;


    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        try {
            preparedStatement.setString(1,this.probeMac);
            preparedStatement.setString(2,this.mobileMac);
            preparedStatement.setString(3,this.signalInten);
            preparedStatement.setString(4,this.wifiType);
            preparedStatement.setString(5,this.timeStamp);
            preparedStatement.setString(6,this.hotelId);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        try {
            this.probeMac=resultSet.getString("probe_mac");
            this.mobileMac=resultSet.getString("mobile_mac");
            this.signalInten=resultSet.getString("signal_inten");
            this.timeStamp=resultSet.getString("time_stamp");
            this.wifiType=resultSet.getString("wifi_type");
            this.hotelId=resultSet.getString("hotel_id");
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
