package com.littlehotspot.hadoop.mr.nginx.mysql.model;

import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 下午 1:48.
 */
@Data
public class HdfsStringModel implements Writable, DBWritable {

    private String result;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out,this.result);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.result = in.readUTF();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {

    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            if(i==1) {
                this.result = rs.getString(i);
            }else {
                this.result = this.result + Constant.VALUE_SPLIT_CHAR + rs.getString(i);
            }
        }
    }
}
