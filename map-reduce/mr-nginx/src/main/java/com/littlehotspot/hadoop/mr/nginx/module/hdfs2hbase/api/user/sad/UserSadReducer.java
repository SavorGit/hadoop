package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
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

    private HBaseHelper hBaseHelper;

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            SadType sadType = SadType.valueOf(conf.get("sadType"));

            UserSad userSad = TargetBeanFactory.getTargetUserSadBean(sadType);
            String rowKey = null;
            TextTargetSadAttrBean targetSadAttrBean = new TextTargetSadAttrBean();
            TextTargetSadRelaBean targetSadRelaBean = new TextTargetSadRelaBean();

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

                rowKey = sourceUserSadBean.getMobile_id() + sourceUserSadBean.getTimestamps();
            }

            if (targetSadAttrBean.getStart() != 0 && targetSadAttrBean.getEnd() != 0) {
                userSad.setRowKey(rowKey);
                userSad.setAttrBean(targetSadAttrBean);
                userSad.setRelaBean(targetSadRelaBean);
                hBaseHelper.insert(userSad);
            }

//            context.write(new Text(value.toString()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.hBaseHelper = new HBaseHelper(conf);
    }

    private void setPropertiesForAttrBean(TextTargetSadAttrBean bean, TextTargetSadActBean source) {
        bean.setDevice_id(source.getMobile_id());
        bean.setType(source.getMedia_type());
        if ("start".equals(source.getOption_type())) {
            bean.setStart(Long.decode(source.getTimestamps().substring(0, 10)));
        } else if ("end".equals(source.getOption_type())) {
            bean.setEnd(Long.decode(source.getTimestamps().substring(0, 10)));
        }
    }

    private void setPropertiesForRelaBean(Configuration conf, TextTargetSadRelaBean bean, TextTargetSadActBean source) throws Exception {
        String hotelId = source.getHotel_id();
        bean.setHotel(hotelId);

        //读取mysql
        Hotel hotel = readMysqlHotel(conf.get("hdfsCluster"), Hotel.class.getName() + hotelId);
        if (hotel != null) {
            bean.setHotel_name(hotel.getName());
        }

        String roomId = source.getRoom_id();
        bean.setRoom(roomId);
        Room room = readMysqlRoom(conf.get("hdfsCluster"), roomId);
        if (room != null) {
            bean.setRoom_name(room.getName());
        }


        String mac = source.getMac();
        bean.setBox_mac(mac);
        Box box = readMysqlBox(conf.get("hdfsCluster"), mac);
        if (box != null && mac.equals(box.getMac())) {
            bean.setBox_name(box.getName());
        }


        String mediaId = source.getMedia_id();
        bean.setMedia(mediaId);
        Media media = readMysqlMedia(conf.get("hdfsCluster"), mediaId);
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
     *
     * @param hdfsCluster
     * @throws Exception
     */
    public Hotel readMysqlHotel(String hdfsCluster, String hid) throws Exception {
        Map<String, Model> map = MysqlCommonVariables.modelMap;
        if (map.get(hid) != null) {
            return (Hotel) map.get(hid);
        }

        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Hotel.class);
        selectModel.setQuery("select id,name from savor_hotel");
        selectModel.setCountQuery("select count(*) from savor_hotel");
        selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

        JdbcReader.readToMap(hdfsCluster, selectModel);

        return (Hotel) MysqlCommonVariables.modelMap.get(hid);

    }

    /**
     * 查询包间信息
     *
     * @param hdfsCluster
     * @throws Exception
     */
    public Room readMysqlRoom(String hdfsCluster, String rid) throws Exception {
        Map<String, Model> map = MysqlCommonVariables.modelMap;
        if (map.get(rid) != null) {
            return (Room) map.get(rid);
        }

        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Room.class);
        selectModel.setQuery("select id,name from savor_room");
        selectModel.setCountQuery("select count(*) from savor_room");
        selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

        JdbcReader.readToMap(hdfsCluster, selectModel);

        return (Room) MysqlCommonVariables.modelMap.get(rid);

    }

    /**
     * 查询机顶盒信息
     *
     * @param hdfsCluster
     * @throws Exception
     */
    public Box readMysqlBox(String hdfsCluster, String mac) throws Exception {
        if (findBox(mac) == null) {
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(Box.class);

            selectModel.setQuery("select id,name,mac from savor_box");
            selectModel.setCountQuery("select count(*) from savor_box");
            selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

            JdbcReader.readToMap(hdfsCluster, selectModel);

            return findBox(mac);
        }

        return findBox(mac);

    }

    public Box findBox(String mac) {
        Map<String, Model> boxMap = MysqlCommonVariables.modelMap;

        for (Map.Entry<String, Model> modelEntry : boxMap.entrySet()) {
            if(modelEntry.getKey().contains(Box.class.getName())) {
                Box box = (Box) modelEntry.getValue();
                if (box != null && mac.equals(box.getMac())) {
                    return box;
                }
            }
        }

        return null;
    }

    /**
     * 查询机顶盒信息
     *
     * @param hdfsCluster
     * @throws Exception
     */
    public Media readMysqlMedia(String hdfsCluster, String mid) throws Exception {
        Map<String, Model> map = MysqlCommonVariables.modelMap;
        if (map.get(mid) != null) {
            return (Media) map.get(mid);
        }

        SelectModel selectModel = new SelectModel();
        selectModel.setInputClass(Media.class);
        selectModel.setQuery("select id,name,oss_addr from savor_media");
        selectModel.setCountQuery("select count(*) from savor_media");
        selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

        JdbcReader.readToMap(hdfsCluster, selectModel);

        return (Media) MysqlCommonVariables.modelMap.get(mid);

    }
}