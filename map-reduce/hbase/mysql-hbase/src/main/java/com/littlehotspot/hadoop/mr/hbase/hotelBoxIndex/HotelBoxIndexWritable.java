package com.littlehotspot.hadoop.mr.hbase.hotelBoxIndex;

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
 * <h1> title </h1>
 * Created by Administrator on 2017-08-11 上午 10:26.
 */
public class HotelBoxIndexWritable extends AbstractWritable implements Writable, DBWritable {
    @Setter
    @Getter
    private Long hotel_id;// 表id

    private Long room_id;//	包间id

    private Long box_id;//	机顶盒id

    @Setter
    @Getter
    private String mac;//	机顶盒mac

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.hotel_id);
        statement.setLong(2, this.room_id);
        statement.setLong(3, this.box_id);
        statement.setString(4, this.mac);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(HotelBoxIndexWritable.class, this, "hotel_id", result, "hotel_id");
        this.setValue(HotelBoxIndexWritable.class, this, "room_id", result, "room_id");
        this.setValue(HotelBoxIndexWritable.class, this, "box_id", result, "box_id");
        this.setValue(HotelBoxIndexWritable.class, this, "mac", result, "mac");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.hotel_id);
        Text.writeString(dataOutput, this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.hotel_id = dataInput.readLong();
        String line = Text.readString(dataInput);
        this.buildBean(line);
    }

    @Override
    public String toString() {
        StringBuilder toString;
        toString = new StringBuilder();
        toString.append(this.hotel_id).append(FIELD_DELIMITER);
        toString.append(this.room_id).append(FIELD_DELIMITER);
        toString.append(this.box_id).append(FIELD_DELIMITER);
        toString.append(this.mac);

        return super.toString();
    }

    public Put toPut() {
        if (this.hotel_id == null) {
            throw new IllegalStateException("The id of hotel-bean for function[toPut]");
        }
        String familyName;
        long version = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(this.hotel_id + "|" + this.mac));// 设置rowkey

        // 基本属性
        familyName = "attr";
        this.addColumn(HotelBoxIndexWritable.class, this, "hotel_id", put, familyName, "hotel_id", version);
        this.addColumn(HotelBoxIndexWritable.class, this, "room_id", put, familyName, "room_id", version);
        this.addColumn(HotelBoxIndexWritable.class, this, "box_id", put, familyName, "box_id", version);
        this.addColumn(HotelBoxIndexWritable.class, this, "mac", put, familyName, "mac", version);

        return put;
    }

    private void buildBean(String line) {
    }
}
