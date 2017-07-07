package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.demand;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.UserSad;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 上午 10:07.
 */
@Data
@HBaseTable(name = "user_demand")
public class UserDemand extends UserSad{
}
