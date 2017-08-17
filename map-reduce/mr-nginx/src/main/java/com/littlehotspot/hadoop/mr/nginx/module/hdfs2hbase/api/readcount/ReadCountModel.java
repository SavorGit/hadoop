package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.readcount;

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

/**
 * Created by gy on 2017/8/2.
 */
@NoArgsConstructor
public class ReadCountModel implements Writable, DBWritable {

    @Setter
    @Getter
    private String conId;

    @Setter
    @Getter
    private String conName;

    @Setter
    @Getter
    private String count;



    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        try {
            preparedStatement.setString(1,this.conId);
            preparedStatement.setString(2,this.conName);
            preparedStatement.setString(3,this.count);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        try {
            this.conId=resultSet.getString("content_id");
            this.conName=resultSet.getString("content_name");
            this.count=resultSet.getString("count");

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
