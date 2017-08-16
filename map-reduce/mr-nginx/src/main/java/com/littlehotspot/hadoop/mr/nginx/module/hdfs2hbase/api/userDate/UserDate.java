package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.userDate;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <h1> 用户按天统计 </h1>
 * Created by Administrator on 2017-08-15 下午 2:46.
 */
@Getter
@Setter
public class UserDate {

    private String device_id;
    private String date;
    private long dema_count;
    private long dema_time;
    private long proje_count;
    private long proje_time;
    private long read_count;
    private long read_time;

    public UserDate(Result result, String date, ActType actType) {
        String deviceId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("device_id")));
        this.device_id = deviceId;
        this.date = date;

        switch (actType) {
            case DEMAND:
                long pTime = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("p_time"))));
                this.dema_time = pTime;
                break;
            case PROJECTION:
                long pTime1 = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("p_time"))));
                this.proje_time = pTime1;
                break;
            case READ:
                long vTime = Long.valueOf(Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("v_time"))));
                this.read_time = vTime;
        }

    }

    public UserDate(ImmutableBytesWritable key) {
        String keyString = Bytes.toString(key.get());
        Pattern regex = Pattern.compile("^(.*)\\|(.*)$");
        Matcher matcher = regex.matcher(keyString);
        if (matcher.find()) {
            this.device_id = matcher.group(1);
            this.date = matcher.group(2);
        }

    }

    public Put toPut(ActType actType) {
        if (this.device_id == null) {
            throw new IllegalStateException("The id of hotel-bean for function[toPut]");
        }
        String familyName;
        Put put = new Put(Bytes.toBytes(this.device_id + "|" + this.date));// 设置rowkey

        // 基本属性
        familyName = "attr";

        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("device_id"), Bytes.toBytes(this.device_id));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("date"), Bytes.toBytes(this.date));

        switch (actType) {
            case DEMAND:
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("dema_count"), Bytes.toBytes(this.dema_count + ""));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("dema_time"), Bytes.toBytes(this.dema_time + ""));
                break;
            case PROJECTION:
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("proje_count"), Bytes.toBytes(this.proje_count + ""));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("proje_time"), Bytes.toBytes(this.proje_time + ""));
                break;
            case READ:
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("read_count"), Bytes.toBytes(this.read_count + ""));
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes("read_time"), Bytes.toBytes(this.read_time + ""));
                break;
        }
        return put;
    }

}
