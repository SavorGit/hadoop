package com.littlehotspot.hadoop.mr.hbase.area;

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
 * Created by Administrator on 2017-09-19 上午 11:06.
 */
public class AreaWritable extends AbstractWritable implements Writable, DBWritable {

    @Setter
    @Getter
    private Long id;

    private Integer check_num;
    private String region_name;
    private String short_name;
    private String zip_code;
    private String area_no;
    private Integer parent_id;
    private Integer contry_id;
    private String full_pinyin;
    private String simple_pinyin;
    private Integer is_valid;
    private Integer is_in_hotel;

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
        statement.setInt(2, this.check_num);
        statement.setString(3, this.region_name);
        statement.setString(4, this.short_name);
        statement.setString(5, this.zip_code);
        statement.setString(6, this.area_no);
        statement.setInt(7, this.parent_id);
        statement.setInt(8, this.contry_id);
        statement.setString(9, this.full_pinyin);
        statement.setString(10, this.simple_pinyin);
        statement.setInt(11, this.is_valid);
        statement.setInt(12, this.is_in_hotel);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(AreaWritable.class, this, "id", result, "id");
        this.setValue(AreaWritable.class, this, "check_num", result, "check_num");
        this.setValue(AreaWritable.class, this, "region_name", result, "region_name");
        this.setValue(AreaWritable.class, this, "short_name", result, "short_name");
        this.setValue(AreaWritable.class, this, "zip_code", result, "zip_code");
        this.setValue(AreaWritable.class, this, "area_no", result, "area_no");
        this.setValue(AreaWritable.class, this, "parent_id", result, "parent_id");
        this.setValue(AreaWritable.class, this, "contry_id", result, "contry_id");
        this.setValue(AreaWritable.class, this, "full_pinyin", result, "full_pinyin");
        this.setValue(AreaWritable.class, this, "simple_pinyin", result, "simple_pinyin");
        this.setValue(AreaWritable.class, this, "is_valid", result, "is_valid");
        this.setValue(AreaWritable.class, this, "is_in_hotel", result, "is_in_hotel");
    }

    @Override
    public String toString() {
        StringBuilder toString;
        toString = new StringBuilder();
        toString.append(this.id).append(FIELD_DELIMITER);
        toString.append(this.check_num).append(FIELD_DELIMITER);
        toString.append(this.region_name).append(FIELD_DELIMITER);
        toString.append(this.short_name).append(FIELD_DELIMITER);
        toString.append(this.zip_code).append(FIELD_DELIMITER);
        toString.append(this.area_no).append(FIELD_DELIMITER);
        toString.append(this.parent_id).append(FIELD_DELIMITER);
        toString.append(this.contry_id).append(FIELD_DELIMITER);
        toString.append(this.full_pinyin).append(FIELD_DELIMITER);
        toString.append(this.simple_pinyin).append(FIELD_DELIMITER);
        toString.append(this.is_valid).append(FIELD_DELIMITER);
        toString.append(this.is_in_hotel);

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
        this.addColumn(AreaWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(AreaWritable.class, this, "check_num", put, familyName, "check_num", version);
        this.addColumn(AreaWritable.class, this, "region_name", put, familyName, "region_name", version);
        this.addColumn(AreaWritable.class, this, "short_name", put, familyName, "short_name", version);
        this.addColumn(AreaWritable.class, this, "zip_code", put, familyName, "zip_code", version);
        this.addColumn(AreaWritable.class, this, "area_no", put, familyName, "area_no", version);
        this.addColumn(AreaWritable.class, this, "parent_id", put, familyName, "parent_id", version);
        this.addColumn(AreaWritable.class, this, "contry_id", put, familyName, "contry_id", version);
        this.addColumn(AreaWritable.class, this, "full_pinyin", put, familyName, "full_pinyin", version);
        this.addColumn(AreaWritable.class, this, "simple_pinyin", put, familyName, "simple_pinyin", version);
        this.addColumn(AreaWritable.class, this, "is_valid", put, familyName, "is_valid", version);
        this.addColumn(AreaWritable.class, this, "is_in_hotel", put, familyName, "is_in_hotel", version);

        return put;
    }

}
