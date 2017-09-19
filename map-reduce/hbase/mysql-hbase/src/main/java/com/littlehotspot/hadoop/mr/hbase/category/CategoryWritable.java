package com.littlehotspot.hadoop.mr.hbase.category;

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
 * Created by Administrator on 2017-09-19 下午 5:56.
 */
public class CategoryWritable extends AbstractWritable implements Writable, DBWritable {

    @Setter
    @Getter
    private Integer id;
    private String name;
    private String en_name;
    private String img_url;
    private Integer sort_num;
    private Long create_time;
    private Long update_time;
    private Long creator_id;
    private Integer state;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        Text.writeString(dataOutput, this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        String line = Text.readString(dataInput);
        this.buildBean(line);
    }

    private void buildBean(String line) {

    }

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.id);
        statement.setString(2, this.name);
        statement.setString(3, this.en_name);
        statement.setString(4, this.img_url);
        statement.setInt(5, this.sort_num);
        statement.setLong(6, this.create_time);
        statement.setLong(7, this.update_time);
        statement.setLong(8, this.creator_id);
        statement.setInt(9, this.state);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(CategoryWritable.class, this, "id", result, "id");
        this.setValue(CategoryWritable.class, this, "name", result, "name");
        this.setValue(CategoryWritable.class, this, "en_name", result, "en_name");
        this.setValue(CategoryWritable.class, this, "img_url", result, "img_url");
        this.setValue(CategoryWritable.class, this, "sort_num", result, "sort_num");
        this.setValue(CategoryWritable.class, this, "create_time", result, "create_time");
        this.setValue(CategoryWritable.class, this, "update_time", result, "update_time");
        this.setValue(CategoryWritable.class, this, "creator_id", result, "creator_id");
        this.setValue(CategoryWritable.class, this, "state", result, "state");
    }

    @Override
    public String toString() {
        StringBuilder toString;
        toString = new StringBuilder();
        toString.append(this.id).append(FIELD_DELIMITER);
        toString.append(this.name).append(FIELD_DELIMITER);
        toString.append(this.en_name).append(FIELD_DELIMITER);
        toString.append(this.img_url).append(FIELD_DELIMITER);
        toString.append(this.sort_num).append(FIELD_DELIMITER);
        toString.append(this.create_time).append(FIELD_DELIMITER);
        toString.append(this.update_time).append(FIELD_DELIMITER);
        toString.append(this.creator_id).append(FIELD_DELIMITER);
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
        this.addColumn(CategoryWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(CategoryWritable.class, this, "name", put, familyName, "name", version);
        this.addColumn(CategoryWritable.class, this, "en_name", put, familyName, "en_name", version);
        this.addColumn(CategoryWritable.class, this, "img_url", put, familyName, "img_url", version);
        this.addColumn(CategoryWritable.class, this, "sort_num", put, familyName, "sort_num", version);
        this.addColumn(CategoryWritable.class, this, "create_time", put, familyName, "create_time", version);
        this.addColumn(CategoryWritable.class, this, "update_time", put, familyName, "update_time", version);
        this.addColumn(CategoryWritable.class, this, "creator_id", put, familyName, "creator_id", version);
        this.addColumn(CategoryWritable.class, this, "state", put, familyName, "state", version);

        return put;
    }
}
