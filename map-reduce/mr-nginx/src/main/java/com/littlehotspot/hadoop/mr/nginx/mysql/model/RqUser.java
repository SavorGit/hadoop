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
 * <h1> 酒店model </h1>
 * Created by Administrator on 2017-06-29 下午 5:38.
 */
@Data
public class RqUser implements Writable, DBWritable {
    private String sourceType;
    private String clientid;
    private String deviceid;
    private String time;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out,this.sourceType);
        Text.writeString(out,this.clientid);
        Text.writeString(out,this.deviceid);
        Text.writeString(out,this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sourceType = in.readUTF();
        this.clientid = in.readUTF();
        this.deviceid = in.readUTF();
        this.time = in.readUTF();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.sourceType);
        stmt.setString(2, this.clientid);
        stmt.setString(3, this.deviceid);
        stmt.setString(4, this.time);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.sourceType = result.getString(1);
        this.clientid = result.getString(2);
        this.deviceid = result.getString(3);
        this.time = result.getString(4)+"000";
    }

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getDeviceid() == null ? "" : this.getDeviceid()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getClientid() == null ? "" : this.getClientid()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append("").append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getTime() == null ? "" : this.getTime()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getSourceType() == null ? "" : this.getSourceType()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append("").append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }

}
