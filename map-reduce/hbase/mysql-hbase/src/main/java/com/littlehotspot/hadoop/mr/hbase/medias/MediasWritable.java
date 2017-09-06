package com.littlehotspot.hadoop.mr.hbase.medias;

import com.littlehotspot.hadoop.mr.hbase.AbstractWritable;
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
 * <h1> 媒体 media </h1>
 * Created by Administrator on 2017-08-28 下午 3:36.
 */
public class MediasWritable extends AbstractWritable implements Writable, DBWritable {

    @Setter
    @Getter
    private Long id;
    private String name;
    private String description;
    private Long create_time;
    private String md5;
    private Integer crt_id;
    private String crt_name;
    private String oss_addr;
    private String file_path;
    private Integer duration;
    private String surfix;
    private Integer type;
    private String oss_etag;
    private Integer state;
    private Long chk_id;
    private String chk_name;
    private Integer flag;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setString(2, this.name);
        statement.setString(3, this.description);
        statement.setLong(4, this.create_time);
        statement.setString(5, this.md5);
        statement.setLong(6, this.crt_id);
        statement.setString(7, this.crt_name);
        statement.setString(8, this.oss_addr);
        statement.setString(9, this.file_path);
        statement.setInt(10, this.duration);
        statement.setString(11, this.surfix);
        statement.setInt(12, this.type);
        statement.setString(13, this.oss_etag);
        statement.setInt(14, this.state);
        statement.setLong(15, this.chk_id);
        statement.setString(16, this.chk_name);
        statement.setInt(17, this.flag);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(MediasWritable.class, this, "id", result, "id");
        this.setValue(MediasWritable.class, this, "name", result, "name");
        this.setValue(MediasWritable.class, this, "description", result, "description");
        this.setValue(MediasWritable.class, this, "create_time", result, "create_time");
        this.setValue(MediasWritable.class, this, "md5", result, "md5");
        this.setValue(MediasWritable.class, this, "crt_id", result, "crt_id");
        this.setValue(MediasWritable.class, this, "crt_name", result, "crt_name");
        this.setValue(MediasWritable.class, this, "oss_addr", result, "oss_addr");
        this.setValue(MediasWritable.class, this, "file_path", result, "file_path");
        this.setValue(MediasWritable.class, this, "duration", result, "duration");
        this.setValue(MediasWritable.class, this, "surfix", result, "surfix");
        this.setValue(MediasWritable.class, this, "type", result, "type");
        this.setValue(MediasWritable.class, this, "oss_etag", result, "oss_etag");
        this.setValue(MediasWritable.class, this, "state", result, "state");
        this.setValue(MediasWritable.class, this, "chk_id", result, "chk_id");
        this.setValue(MediasWritable.class, this, "chk_name", result, "chk_name");
        this.setValue(MediasWritable.class, this, "flag", result, "flag");
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
        toString.append(this.description).append(FIELD_DELIMITER);
        toString.append(this.create_time).append(FIELD_DELIMITER);
        toString.append(this.md5).append(FIELD_DELIMITER);
        toString.append(this.crt_id).append(FIELD_DELIMITER);
        toString.append(this.crt_name).append(FIELD_DELIMITER);
        toString.append(this.oss_addr).append(FIELD_DELIMITER);
        toString.append(this.file_path).append(FIELD_DELIMITER);
        toString.append(this.duration).append(FIELD_DELIMITER);
        toString.append(this.surfix).append(FIELD_DELIMITER);
        toString.append(this.type).append(FIELD_DELIMITER);
        toString.append(this.oss_etag).append(FIELD_DELIMITER);
        toString.append(this.state).append(FIELD_DELIMITER);
        toString.append(this.chk_id).append(FIELD_DELIMITER);
        toString.append(this.chk_name).append(FIELD_DELIMITER);
        toString.append(this.flag);

        return super.toString();
    }

    public Put toPut() {
        if (this.id == null) {
            throw new IllegalStateException("The id of media-bean for function[toPut]");
        }
        String familyName;
        long version = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(this.id + ""));// 设置rowkey

        // 基本属性
        familyName = "attr";
        this.addColumn(MediasWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(MediasWritable.class, this, "name", put, familyName, "name", version);
        this.addColumn(MediasWritable.class, this, "description", put, familyName, "description", version);
        this.addColumn(MediasWritable.class, this, "create_time", put, familyName, "create_time", version);
        this.addColumn(MediasWritable.class, this, "md5", put, familyName, "md5", version);
        this.addColumn(MediasWritable.class, this, "crt_id", put, familyName, "crt_id", version);
        this.addColumn(MediasWritable.class, this, "crt_name", put, familyName, "crt_name", version);
        this.addColumn(MediasWritable.class, this, "oss_addr", put, familyName, "oss_addr", version);
        this.addColumn(MediasWritable.class, this, "file_path", put, familyName, "file_path", version);
        this.addColumn(MediasWritable.class, this, "duration", put, familyName, "duration", version);
        this.addColumn(MediasWritable.class, this, "surfix", put, familyName, "surfix", version);
        this.addColumn(MediasWritable.class, this, "type", put, familyName, "type", version);
        this.addColumn(MediasWritable.class, this, "oss_etag", put, familyName, "oss_etag", version);
        this.addColumn(MediasWritable.class, this, "state", put, familyName, "state", version);
        this.addColumn(MediasWritable.class, this, "chk_id", put, familyName, "chk_id", version);
        this.addColumn(MediasWritable.class, this, "chk_name", put, familyName, "chk_name", version);
        this.addColumn(MediasWritable.class, this, "flag", put, familyName, "flag", version);

        return put;
    }

    private void buildBean(String line) {

    }
}
