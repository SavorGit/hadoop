package com.littlehotspot.hadoop.mr.nginx.mysql.model;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

/**
 * <h1> mysql查询base类 </h1>
 * Created by Administrator on 2017-06-30 下午 2:03.
 */
public abstract class Model implements Writable, DBWritable {
    public abstract String getId();

    public abstract String toString();
}
