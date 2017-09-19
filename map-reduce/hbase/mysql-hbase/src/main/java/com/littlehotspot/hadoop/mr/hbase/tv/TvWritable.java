package com.littlehotspot.hadoop.mr.hbase.tv;

import com.littlehotspot.hadoop.mr.hbase.AbstractWritable;
import com.littlehotspot.hadoop.mr.hbase.area.AreaWritable;
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
 * Created by Administrator on 2017-09-19 上午 11:23.
 */
public class TvWritable extends AbstractWritable implements Writable, DBWritable {
    @Setter
    @Getter
    private Long id;
    private String tv_brand;
    private String tv_size;
    private Integer tv_source;
    private Long box_id;
    private Integer flag;
    private Integer state;


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

    private void buildBean(String line) {

    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setString(2, this.tv_brand);
        statement.setString(3, this.tv_size);
        statement.setInt(4, this.tv_source);
        statement.setLong(5, this.box_id);
        statement.setInt(6, this.flag);
        statement.setInt(7, this.state);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(TvWritable.class, this, "id", result, "id");
        this.setValue(TvWritable.class, this, "tv_brand", result, "tv_brand");
        this.setValue(TvWritable.class, this, "tv_size", result, "tv_size");
        this.setValue(TvWritable.class, this, "tv_source", result, "tv_source");
        this.setValue(TvWritable.class, this, "box_id", result, "box_id");
        this.setValue(TvWritable.class, this, "flag", result, "flag");
        this.setValue(TvWritable.class, this, "state", result, "state");
    }

    @Override
    public String toString() {
        StringBuilder toString;
        toString = new StringBuilder();
        toString.append(this.id).append(FIELD_DELIMITER);
        toString.append(this.tv_brand).append(FIELD_DELIMITER);
        toString.append(this.tv_size).append(FIELD_DELIMITER);
        toString.append(this.tv_source).append(FIELD_DELIMITER);
        toString.append(this.box_id).append(FIELD_DELIMITER);
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
        Put put = new Put(Bytes.toBytes(this.id + ""));// 设置rowkey

        // 基本属性
        familyName = "attr";
        this.addColumn(TvWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(TvWritable.class, this, "tv_brand", put, familyName, "tv_brand", version);
        this.addColumn(TvWritable.class, this, "tv_size", put, familyName, "tv_size", version);
        this.addColumn(TvWritable.class, this, "tv_source", put, familyName, "tv_source", version);
        this.addColumn(TvWritable.class, this, "box_id", put, familyName, "box_id", version);
        this.addColumn(TvWritable.class, this, "flag", put, familyName, "flag", version);
        this.addColumn(TvWritable.class, this, "state", put, familyName, "state", version);

        return put;
    }
}
