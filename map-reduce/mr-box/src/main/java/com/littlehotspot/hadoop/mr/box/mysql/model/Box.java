package com.littlehotspot.hadoop.mr.box.mysql.model;

import lombok.Data;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <h1> 机顶盒model </h1>
 * Created by Administrator on 2017-06-30 下午 4:36.
 */
@Data
public class Box extends Model {
    private String id;
    private String name;
    private String mac;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out,this.id);
        Text.writeString(out,this.name);
        Text.writeString(out,this.mac);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.mac = in.readUTF();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.id);
        stmt.setString(2, this.name);
        stmt.setString(3, this.mac);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.id = result.getString(1);
        this.name = result.getString(2);
        this.mac = result.getString(3);
    }

}
