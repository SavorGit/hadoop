package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * <h1> 用户行为最终处理并插入到 Hbase </h1>
 * Created by Administrator on 2017-06-27 下午 3:05.
 */
public class UserSadReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            SadType sadType = SadType.valueOf(conf.get("sadType"));

            TextTargetSadAttrBean targetSadAttrBean = TargetBeanFactory.getTargetSadAttrBean(sadType);
            TextTargetSadRelaBean targetSadRelaBean = TargetBeanFactory.getTargetSadRelaBean(sadType);

            Iterator<Text> textIterator = value.iterator();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                String rowLineContent = item.toString();
                TextTargetSadActBean sourceUserSadBean = new TextTargetSadActBean(rowLineContent);

                this.setPropertiesForAttrBean(targetSadAttrBean, sourceUserSadBean);
                this.setPropertiesForRelaBean(conf, targetSadRelaBean, sourceUserSadBean);

            }

            if(targetSadAttrBean.getStart() != 0 && targetSadAttrBean.getEnd() != 0) {
                CommonVariables.hBaseHelper.insert(targetSadAttrBean);
                CommonVariables.hBaseHelper.insert(targetSadRelaBean);
            }

//            context.write(new Text(value.toString()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setPropertiesForAttrBean(TextTargetSadAttrBean bean, TextTargetSadActBean source) {
        bean.setRowKey(source.getMobile_id() + source.getTimestamps());
        bean.setDevice_id(source.getMobile_id());
        bean.setType(source.getMedia_type());
        if ("start".equals(source.getOption_type())) {
            bean.setStart(Long.decode(source.getTimestamps().substring(0, 10)));
        } else if ("end".equals(source.getOption_type())) {
            bean.setEnd(Long.decode(source.getTimestamps().substring(0, 10)));
        }
    }

    private void setPropertiesForRelaBean(Configuration conf, TextTargetSadRelaBean bean, TextTargetSadActBean source) throws Exception {
        bean.setRowKey(source.getMobile_id() + source.getTimestamps());

        String hotelId = source.getHotel_id();
        bean.setHotel(hotelId);

        //读取mysql
        readMysqlHotel(conf.get("hdfsCluster"));
        Hotel hotel = (Hotel) MysqlCommonVariables.modelMap.get(hotelId);
        if(hotel != null) {
            bean.setHotel_name(hotel.getName());
        }

        readMysqlRoom(conf.get("hdfsCluster"));
        String roomId = source.getRoom_id();
        bean.setRoom(roomId);
        Room room = (Room) MysqlCommonVariables.modelMap.get(roomId);
        if(room != null) {
            bean.setRoom_name(room.getName());
        }

        readMysqlBox(conf.get("hdfsCluster"));
        bean.setBox_mac(source.getMac());
        Map<String,Model> boxMap = MysqlCommonVariables.modelMap;
        for (Map.Entry<String, Model> modelEntry : boxMap.entrySet()) {
            Box box = (Box) modelEntry.getValue();
            if(box != null && source.getMac().equals(box.getMac())){
                bean.setBox_name(box.getName());
            }
        }

        readMysqlMedia(conf.get("hdfsCluster"));
        bean.setMedia(source.getMedia_id());
        Media media = (Media) MysqlCommonVariables.modelMap.get(source.getMedia_id());
        if(media != null) {
            bean.setMedia_name(media.getName());
            bean.setMedia_down_url(media.getDownloadUrl());
        }

        bean.setApk_version(source.getApk_version());
        bean.setAds_version(source.getAds_period());
        bean.setDema_version(source.getDemand_period());
    }


    /**
     * 查询酒店信息
     * @param hdfsCluster
     * @throws Exception
     */
    public void readMysqlHotel(String hdfsCluster) throws Exception{
        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Hotel.class);
        selectModel.setTableName("savor_hotel");
        selectModel.setFields(MysqlCommonVariables.hotelFields);
        selectModel.setOutput("/home/data/hadoop/flume/test_hbase/mysql");

        JdbcReader.read(hdfsCluster,selectModel);

    }

    /**
     * 查询包间信息
     * @param hdfsCluster
     * @throws Exception
     */
    public void readMysqlRoom(String hdfsCluster) throws Exception{
        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Room.class);
        selectModel.setTableName("savor_room");
        selectModel.setFields(MysqlCommonVariables.roomFields);
        selectModel.setOutput("/home/data/hadoop/flume/test_hbase/mysql");

        JdbcReader.read(hdfsCluster,selectModel);

    }

    /**
     * 查询机顶盒信息
     * @param hdfsCluster
     * @throws Exception
     */
    public void readMysqlBox(String hdfsCluster) throws Exception{
        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Box.class);
        selectModel.setTableName("savor_box");
        selectModel.setFields(MysqlCommonVariables.boxFields);
        selectModel.setOutput("/home/data/hadoop/flume/test_hbase/mysql");

        JdbcReader.read(hdfsCluster,selectModel);

    }

    /**
     * 查询机顶盒信息
     * @param hdfsCluster
     * @throws Exception
     */
    public void readMysqlMedia(String hdfsCluster) throws Exception{
        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Media.class);
        selectModel.setTableName("savor_media");
        selectModel.setFields(MysqlCommonVariables.mediaFields);
        selectModel.setOutput("/home/data/hadoop/flume/test_hbase/mysql");

        JdbcReader.read(hdfsCluster,selectModel);

    }
}