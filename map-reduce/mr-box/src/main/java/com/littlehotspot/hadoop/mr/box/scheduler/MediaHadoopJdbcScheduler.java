package com.littlehotspot.hadoop.mr.box.scheduler;

import com.littlehotspot.hadoop.mr.box.mysql.model.SelectModel;
import com.littlehotspot.hadoop.mr.box.mysql.model.Media;

/**
 *@Author 刘飞飞
 *@Date 2017/7/7 17:42
 */
public class MediaHadoopJdbcScheduler extends HadoopJdbc{
    @Override
    public SelectModel bindSelectModel() {
        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Media.class);
        selectModel.setQuery("select id,name from savor_media");
        selectModel.setCountQuery("select count(*) from savor_media");
        selectModel.setOutputPath("/home/data/hadoop/flume/tmp/mysql/media");
        return selectModel;
    }
}
