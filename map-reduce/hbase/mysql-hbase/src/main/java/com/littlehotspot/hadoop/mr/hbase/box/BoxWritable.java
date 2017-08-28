package com.littlehotspot.hadoop.mr.hbase.box;

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
 * Created by Administrator on 2017-08-07 下午 3:37.
 */
public class BoxWritable extends AbstractWritable implements Writable, DBWritable {
    @Setter
    @Getter
    private Long id;// 表id

    private Long room_id;//	包间id
    private String name;// 机顶盒名称

    @Setter
    @Getter
    private String mac;// mac地址
    private Integer switch_time;// 切换时间
    private Integer volum;//	音量
    private Integer state;//	状态
    private Integer flag;// 状态标识
    private Long create_time;// 创建时间
    private Long update_time;// 更新时间

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setLong(2, this.room_id);
        statement.setString(3, this.name);
        statement.setString(4, this.mac);
        statement.setInt(5, this.switch_time);
        statement.setInt(6, this.volum);
        statement.setInt(7, this.state);
        statement.setInt(8, this.flag);
        statement.setLong(9, this.create_time);
        statement.setLong(10, this.update_time);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(BoxWritable.class, this, "id", result, "id");
        this.setValue(BoxWritable.class, this, "room_id", result, "room_id");
        this.setValue(BoxWritable.class, this, "name", result, "name");
        this.setValue(BoxWritable.class, this, "mac", result, "mac");
        this.setValue(BoxWritable.class, this, "switch_time", result, "switch_time");
        this.setValue(BoxWritable.class, this, "volum", result, "volum");
        this.setValue(BoxWritable.class, this, "state", result, "state");
        this.setValue(BoxWritable.class, this, "flag", result, "flag");
        this.setValue(BoxWritable.class, this, "create_time", result, "create_time");
        this.setValue(BoxWritable.class, this, "update_time", result, "update_time");
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
        toString.append(this.room_id).append(FIELD_DELIMITER);
        toString.append(this.name).append(FIELD_DELIMITER);
        toString.append(this.mac).append(FIELD_DELIMITER);
        toString.append(this.switch_time).append(FIELD_DELIMITER);
        toString.append(this.volum).append(FIELD_DELIMITER);
        toString.append(this.state).append(FIELD_DELIMITER);
        toString.append(this.flag).append(FIELD_DELIMITER);
        toString.append(this.create_time).append(FIELD_DELIMITER);
        toString.append(this.update_time);

        return super.toString();
    }

    public Put toPut() {
        if (this.id == null) {
            throw new IllegalStateException("The id of hotel-bean for function[toPut]");
        }
        String familyName;
        long version = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(this.id + "|" + this.mac));// 设置rowkey

        // 基本属性
        familyName = "attr";
        this.addColumn(BoxWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(BoxWritable.class, this, "room_id", put, familyName, "room_id", version);
        this.addColumn(BoxWritable.class, this, "name", put, familyName, "name", version);
        this.addColumn(BoxWritable.class, this, "mac", put, familyName, "mac", version);
        this.addColumn(BoxWritable.class, this, "switch_time", put, familyName, "switch_time", version);
        this.addColumn(BoxWritable.class, this, "volum", put, familyName, "volum", version);
        this.addColumn(BoxWritable.class, this, "state", put, familyName, "state", version);
        this.addColumn(BoxWritable.class, this, "flag", put, familyName, "flag", version);
        this.addColumn(BoxWritable.class, this, "create_time", put, familyName, "create_time", version);
        this.addColumn(BoxWritable.class, this, "update_time", put, familyName, "update_time", version);

        return put;
    }

    private void buildBean(String line) {

    }

}
