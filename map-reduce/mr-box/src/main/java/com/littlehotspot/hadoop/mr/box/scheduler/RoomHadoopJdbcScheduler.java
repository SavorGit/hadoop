package com.littlehotspot.hadoop.mr.box.scheduler;

import com.littlehotspot.hadoop.mr.box.mysql.model.Room;
import com.littlehotspot.hadoop.mr.box.mysql.model.SelectModel;

/**
 *@Author 刘飞飞
 *@Date 2017/7/7 17:42
 */
public class RoomHadoopJdbcScheduler extends HadoopJdbc{
    @Override
    public SelectModel bindSelectModel() {
        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Room.class);
        selectModel.setQuery("select id,name from savor_room");
        selectModel.setCountQuery("select count(*) from savor_room");
        selectModel.setOutputPath("/home/data/hadoop/flume/tmp/mysql/room");
        return selectModel;
    }
}
