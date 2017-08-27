package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.tags;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Created by gy on 2017/8/2.
 */
@NoArgsConstructor
public class TagModel implements Writable, DBWritable {



    @Setter
    @Getter
    private int article_id;

    @Setter
    @Getter
    private int tagid;

    @Setter
    @Getter
    private String tagname;


    public String toString() {
        StringBuffer toString = new StringBuffer();
        toString.append(this.article_id).append(",");
        toString.append(this.tagid).append(",");
        toString.append(this.tagname);
        return toString.toString();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1,this.article_id);
        preparedStatement.setInt(2,this.tagid);
        preparedStatement.setString(3,this.tagname);

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.article_id=resultSet.getInt("article_id");
        this.tagid=resultSet.getInt("tagid");
        this.tagname=resultSet.getString("tagname");
    }
}
