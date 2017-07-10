package com.littlehotspot.hadoop.mr.nginx.mysql.model;

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
public class Tags implements Writable, DBWritable {
    private String articleId;
    private String tagid;
    private String tagname;

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out,this.articleId);
        Text.writeString(out,this.tagid);
        Text.writeString(out,this.tagname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.articleId = in.readUTF();
        this.tagid = in.readUTF();
        this.tagname = in.readUTF();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.articleId);
        stmt.setString(2, this.tagid);
        stmt.setString(3, this.tagname);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.articleId = result.getString(1);
        this.tagid = result.getString(2);
        this.tagname = result.getString(3);
    }

}
