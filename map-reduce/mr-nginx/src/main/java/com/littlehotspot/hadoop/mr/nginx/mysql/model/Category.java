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
public class Category{
    private int id;
    private String name;



}
