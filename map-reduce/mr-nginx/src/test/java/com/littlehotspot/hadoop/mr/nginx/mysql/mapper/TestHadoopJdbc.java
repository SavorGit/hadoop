package com.littlehotspot.hadoop.mr.nginx.mysql.mapper;

import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.Hotel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;
import org.junit.Test;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-30 上午 11:31.
 */
public class TestHadoopJdbc {
    @Test
    public void testJdbc(){
        System.setProperty("hadoop.home.dir", "E:\\DevpPrograms\\hadoop-2.7.3");

        try {
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(Hotel.class);
            selectModel.setTableName("savor_hotel");
            selectModel.setFields(MysqlCommonVariables.hotelFields);
            selectModel.setOutput("/home/data/hadoop/flume/test_hbase/mysql");

            JdbcReader.read("hdfs://devpd1:8020",selectModel);
            System.out.println(MysqlCommonVariables.modelMap);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
