package com.littlehotspot.hadoop.mr.box.scheduler;

import com.littlehotspot.hadoop.mr.box.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.box.mysql.model.SelectModel;

/**
 *@Author 刘飞飞
 *@Date 2017/7/7 17:37
 */
public abstract class HadoopJdbc {
    private SelectModel selectModel;
    public abstract SelectModel bindSelectModel();
    public void jdbcToMap(String defaultFs){
        try {
            this.selectModel=bindSelectModel();
            JdbcReader.readToMap(defaultFs,this.selectModel);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
