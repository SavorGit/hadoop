package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.io;

import lombok.Data;
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
 * <h1> title </h1>
 * Created by Administrator on 2017-09-06 下午 5:56.
 */
@Data
public class ContentLogWritable implements Writable, DBWritable {

    private static final char FIELD_DELIMITER = 0x0001;

    private String ip;

    private String isWx;

    private String netType;

    private String deviceType;

    private long timestamp;

    private String contentId;

    private String channel;

    private String isSq;

    private String requestUrl;

    @Override
    public String toString() {
        StringBuffer toString = new StringBuffer();
        toString.append(this.ip).append(FIELD_DELIMITER);
        toString.append(this.isWx).append(FIELD_DELIMITER);
        toString.append(this.netType).append(FIELD_DELIMITER);
        toString.append(this.deviceType).append(FIELD_DELIMITER);
        toString.append(this.timestamp).append(FIELD_DELIMITER);
        toString.append(this.contentId).append(FIELD_DELIMITER);
        toString.append(this.channel).append(FIELD_DELIMITER);
        toString.append(this.isSq).append(FIELD_DELIMITER);
        toString.append(this.requestUrl);

        return toString.toString();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.ip);
        stmt.setString(2, this.isWx);
        stmt.setString(3, this.netType);
        stmt.setString(4, this.deviceType);
        stmt.setLong(5, this.timestamp);
        stmt.setString(6, this.contentId);
        stmt.setString(7, this.channel);
        stmt.setString(8, this.isSq);
        stmt.setString(9, this.requestUrl);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.ip = result.getString("ip");
        this.isWx = result.getString("is_wx");
        this.netType = result.getString("net_type");
        this.deviceType = result.getString("device_type");
        this.timestamp = result.getLong("timestamp");
        this.contentId = result.getString("content_id");
        this.channel = result.getString("channel");
        this.isSq = result.getString("is_sq");
        this.requestUrl = result.getString("request_url");
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String line = Text.readString(in);
        this.buildBean(line);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.toString());
    }

    private void buildBean(String line) {
    }
}
