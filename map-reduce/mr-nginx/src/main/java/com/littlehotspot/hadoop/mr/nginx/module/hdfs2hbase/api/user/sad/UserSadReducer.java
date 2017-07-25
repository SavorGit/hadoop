package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorBox;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorHotel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorMedia;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorRoom;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import com.littlehotspot.hadoop.mr.nginx.util.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <h1> 用户行为最终处理并插入到 Hbase </h1>
 * Created by Administrator on 2017-06-27 下午 3:05.
 */
public class UserSadReducer extends Reducer<Text, Text, Text, Text> {

    private HBaseHelper hBaseHelper;

    private Map<String, Object> hotelMap = new ConcurrentHashMap<>();

    private Map<String, Object> roomMap = new ConcurrentHashMap<>();

    private Map<String, Object> boxMap = new ConcurrentHashMap<>();

    private Map<String, Object> mediaMap = new ConcurrentHashMap<>();

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

                if ("start".equals(sourceUserSadBean.getOption_type())) {
                    this.setPropertiesForAttrBean(targetSadAttrBean, sourceUserSadBean);
                    this.setPropertiesForRelaBean(targetSadRelaBean, sourceUserSadBean);
                } else if ("end".equals(sourceUserSadBean.getOption_type())) {
                    targetSadAttrBean.setEnd(Long.decode(sourceUserSadBean.getTimestamps()));
                }

                rowKey = sourceUserSadBean.getMobile_id() + Constant.ROWKEY_SPLIT_CHAR + (9999999999L - Long.valueOf(sourceUserSadBean.getTimestamps().substring(0, 10)));
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

        String hotels = conf.get("hotels");
        String rooms = conf.get("rooms");
        String boxes = conf.get("boxes");
        String medias = conf.get("medias");

        List<Object> hotelList = JSONUtil.JSONArrayToList(hotels, SavorHotel.class);
        for (Object o : hotelList) {
            SavorHotel hotel = (SavorHotel) o;
            this.hotelMap.put(String.valueOf(hotel.getId()), hotel);
        }

        List<Object> roomList = JSONUtil.JSONArrayToList(rooms, SavorRoom.class);
        for (Object o : roomList) {
            SavorRoom room = (SavorRoom) o;
            this.roomMap.put(String.valueOf(room.getId()), room);
        }

        List<Object> boxList = JSONUtil.JSONArrayToList(boxes, SavorBox.class);
        for (Object o : boxList) {
            SavorBox box = (SavorBox) o;
            this.boxMap.put(String.valueOf(box.getMac()), box);
        }

        List<Object> mediaList = JSONUtil.JSONArrayToList(medias, SavorMedia.class);
        for (Object o : mediaList) {
            SavorMedia media = (SavorMedia) o;
            this.mediaMap.put(String.valueOf(media.getId()), media);
        }

    }

    /**
     * 设置基本属性
     *
     * @param bean
     * @param source
     */
    private void setPropertiesForAttrBean(TextTargetSadAttrBean bean, TextTargetSadActBean source) {
        bean.setDevice_id(source.getMobile_id());
        bean.setType(source.getCommon_value());
        if ("start".equals(source.getOption_type())) {
            bean.setStart(Long.decode(source.getTimestamps()));
        } else if ("end".equals(source.getOption_type())) {
            bean.setEnd(Long.decode(source.getTimestamps()));
        }
    }

    /**
     * 设置关联属性
     *
     * @param bean
     * @param source
     * @throws Exception
     */
    private void setPropertiesForRelaBean(TextTargetSadRelaBean bean, TextTargetSadActBean source) throws Exception {
        String hotelId = source.getHotel_id();
        bean.setHotel(hotelId);

        //读取mysql
        SavorHotel hotel = this.getHotel(hotelId);
        if (hotel != null) {
            bean.setHotel_name(hotel.getName());
        }

        String roomId = source.getRoom_id();
        bean.setRoom(roomId);
        SavorRoom room = this.getRoom(roomId);
        if (room != null) {
            bean.setRoom_name(room.getName());
        }

        String mac = source.getMac();
        bean.setBox_mac(mac);
        SavorBox box = this.getBox(mac);
        if (box != null && mac.equals(box.getMac())) {
            bean.setBox_name(box.getName());
        }

        String mediaId = source.getMedia_id();
        bean.setMedia(mediaId);
        SavorMedia media = this.getMedia(mediaId);
        if (media != null) {
            bean.setMedia_name(media.getName());
            bean.setMedia_down_url(media.getOss_addr());
        }

        bean.setApk_version(source.getApk_version());
        bean.setAds_version(source.getAds_period());
        bean.setDema_version(source.getDemand_period());
    }


    /**
     * 查询酒店信息
     *
     * @throws Exception
     */
    private SavorHotel getHotel(String hid) throws Exception {
        if (this.hotelMap == null || this.hotelMap.get(hid) == null || this.hotelMap.size() <= 0) {
            return null;
        }
        return (SavorHotel) this.hotelMap.get(hid);
    }

    /**
     * 查询包间信息
     *
     * @throws Exception
     */
    private SavorRoom getRoom(String rid) throws Exception {
        if (this.roomMap == null || this.roomMap.get(rid) == null || this.roomMap.size() <= 0) {
            return null;
        }

        return (SavorRoom) this.roomMap.get(rid);
    }

    /**
     * 查询机顶盒信息
     *
     * @throws Exception
     */
    private SavorBox getBox(String mac) throws Exception {
        if (this.boxMap == null || this.boxMap.get(mac) == null || this.boxMap.size() <= 0) {
            return null;
        }

        return (SavorBox) this.boxMap.get(mac);
    }

    /**
     * 查询媒体信息
     *
     * @throws Exception
     */
    private SavorMedia getMedia(String mid) throws Exception {
        if (this.mediaMap == null || this.mediaMap.get(mid) == null || this.mediaMap.size() <= 0) {
            return null;
        }

        return (SavorMedia) this.mediaMap.get(mid);
    }

}