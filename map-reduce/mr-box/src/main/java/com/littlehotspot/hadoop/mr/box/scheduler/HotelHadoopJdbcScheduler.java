package com.littlehotspot.hadoop.mr.box.scheduler;
import com.littlehotspot.hadoop.mr.box.mysql.model.Hotel;
import com.littlehotspot.hadoop.mr.box.mysql.model.SelectModel;

/**
 *@Author 刘飞飞
 *@Date 2017/7/7 17:42
 */
public class HotelHadoopJdbcScheduler extends HadoopJdbc{
    @Override
    public SelectModel bindSelectModel() {
        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Hotel.class);
        selectModel.setQuery("select id,name from savor_hotel");
        selectModel.setCountQuery("select count(*) from savor_hotel");
        selectModel.setOutputPath("/home/data/hadoop/flume/tmp/mysql/hotel");
        return selectModel;
    }
}
