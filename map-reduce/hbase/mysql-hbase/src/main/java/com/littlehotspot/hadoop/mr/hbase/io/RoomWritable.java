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
 * <h1> title </h1>
 * Created by Administrator on 2017-08-07 上午 10:35.
 */
public class RoomWritable extends AbstractWritable implements Writable, DBWritable {
    @Setter
    @Getter
    private Long id;// 表id
    private Long hotel_id;// 酒店id
    private String name; // 包间名称
    private Integer type; // 包间类型
    private String remark; // 备注
    private Long create_time; // 创建时间
    private Long update_time; // 更新时间
    private Integer flag; // 状态标识
    private Integer state; // 状态

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setLong(2, this.hotel_id);
        statement.setString(3, this.name);
        statement.setInt(4, this.type);
        statement.setString(5, this.remark);
        statement.setLong(6, this.create_time);
        statement.setLong(7, this.update_time);
        statement.setInt(8, this.flag);
        statement.setInt(9, this.state);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(RoomWritable.class, this, "id", result, "id");
        this.setValue(RoomWritable.class, this, "hotel_id", result, "hotel_id");
        this.setValue(RoomWritable.class, this, "name", result, "name");
        this.setValue(RoomWritable.class, this, "type", result, "type");
        this.setValue(RoomWritable.class, this, "remark", result, "remark");
        this.setValue(RoomWritable.class, this, "create_time", result, "create_time");
        this.setValue(RoomWritable.class, this, "update_time", result, "update_time");
        this.setValue(RoomWritable.class, this, "flag", result, "flag");
        this.setValue(RoomWritable.class, this, "state", result, "state");
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
        toString.append(this.hotel_id).append(FIELD_DELIMITER);
        toString.append(this.name).append(FIELD_DELIMITER);
        toString.append(this.type).append(FIELD_DELIMITER);
        toString.append(this.remark).append(FIELD_DELIMITER);
        toString.append(this.create_time).append(FIELD_DELIMITER);
        toString.append(this.update_time).append(FIELD_DELIMITER);
        toString.append(this.flag).append(FIELD_DELIMITER);
        toString.append(this.state);

        return super.toString();
    }

    public Put toPut() {
        if (this.id == null) {
            throw new IllegalStateException("The id of hotel-bean for function[toPut]");
        }
        String familyName;
        long version = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(this.id));// 设置rowkey

        // 基本属性
        familyName = "attr";
        this.addColumn(RoomWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(RoomWritable.class, this, "hotel_id", put, familyName, "hotel_id", version);
        this.addColumn(RoomWritable.class, this, "name", put, familyName, "name", version);
        this.addColumn(RoomWritable.class, this, "type", put, familyName, "type", version);
        this.addColumn(RoomWritable.class, this, "remark", put, familyName, "remark", version);
        this.addColumn(RoomWritable.class, this, "create_time", put, familyName, "create_time", version);
        this.addColumn(RoomWritable.class, this, "update_time", put, familyName, "update_time", version);
        this.addColumn(RoomWritable.class, this, "flag", put, familyName, "flag", version);
        this.addColumn(RoomWritable.class, this, "state", put, familyName, "state", version);

        return put;
    }

    private void buildBean(String line) {
    }


}
