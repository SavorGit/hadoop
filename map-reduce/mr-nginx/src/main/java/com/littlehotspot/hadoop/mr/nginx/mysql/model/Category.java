package com.littlehotspot.hadoop.mr.nginx.mysql.model;

import lombok.Data;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <h1> 酒店model </h1>
 * Created by Administrator on 2017-06-29 下午 5:38.
 */
@Data
public class Category extends Model {
    private String id;
    private String name;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out,this.id);
        Text.writeString(out,this.name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.name = in.readUTF();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.id);
        stmt.setString(2, this.name);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.id = result.getString(1);
        this.name = result.getString(2);
    }

}
