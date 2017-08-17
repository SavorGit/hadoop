package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.basic;

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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * Created by gy on 2017/8/2.
 */
@NoArgsConstructor
public class RqUserModel implements Writable, DBWritable {

    private static final char FIELD_DELIMITER = 0x0001;

    @Setter
    @Getter
    private int id;

    @Setter
    @Getter
    private int source_type;

    @Setter
    @Getter
    private int clientid;

    @Setter
    @Getter
    private String deviceid;

    @Setter
    @Getter
    private int dowload_device_id;

    @Setter
    @Getter
    private int hotelId;

    @Setter
    @Getter
    private int waiterid;

    @Setter
    @Getter
    private String addtime;



    public String toString() {
        StringBuffer toString = new StringBuffer();
        toString.append(this.deviceid).append(FIELD_DELIMITER);
        if (this.clientid==1){
            toString.append("android").append(FIELD_DELIMITER);
        }else if (this.clientid==2){
            toString.append("ios").append(FIELD_DELIMITER);
        }
        toString.append("").append(FIELD_DELIMITER);
        try {
            String time =this.addtime.substring(0,19);
            String dateToStamp = dateToStamp(time);
            toString.append(dateToStamp).append(FIELD_DELIMITER);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        toString.append(this.source_type).append(FIELD_DELIMITER);
        toString.append("").append(FIELD_DELIMITER);
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
        preparedStatement.setInt(1,this.id);
        preparedStatement.setInt(2,this.source_type);
        preparedStatement.setInt(3,this.clientid);
        preparedStatement.setString(4,this.deviceid);
        preparedStatement.setInt(5,this.dowload_device_id);
        preparedStatement.setInt(6,this.hotelId);
        preparedStatement.setInt(7,this.waiterid);
        preparedStatement.setString(8,this.addtime);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id=resultSet.getInt("id");
        this.source_type=resultSet.getInt("source_type");
        this.clientid=resultSet.getInt("clientid");
        this.deviceid=resultSet.getString("deviceid");
        this.dowload_device_id=resultSet.getInt("dowload_device_id");
        this.hotelId=resultSet.getInt("hotelid");
        this.waiterid=resultSet.getInt("waiterid");
        this.addtime=resultSet.getString("add_time");
    }

    /*
     * 将时间转换为时间戳
     */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = (Date) simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }
}
