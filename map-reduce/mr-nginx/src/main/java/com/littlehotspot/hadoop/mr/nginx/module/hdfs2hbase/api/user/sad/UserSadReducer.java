package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorBox;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorHotel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorMedia;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorRoom;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.SQLException;
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

                this.setPropertiesForAttrBean(targetSadAttrBean, sourceUserSadBean);
                this.setPropertiesForRelaBean(conf, targetSadRelaBean, sourceUserSadBean);

                rowKey = sourceUserSadBean.getMobile_id() + Constant.ROWKEY_SPLIT_CHAR + sourceUserSadBean.getTimestamps();
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
        SavorHotel hotel = readMysqlHotel(hotelId);
        if (hotel != null) {
            bean.setHotel_name(hotel.getName());
        }

        String roomId = source.getRoom_id();
        bean.setRoom(roomId);
        SavorRoom room = readMysqlRoom(roomId);
        if (room != null) {
            bean.setRoom_name(room.getName());
        }


        String mac = source.getMac();
        bean.setBox_mac(mac);
        SavorBox box = readMysqlBox(mac);
        if (box != null && mac.equals(box.getMac())) {
            bean.setBox_name(box.getName());
        }


        String mediaId = source.getMedia_id();
        bean.setMedia(mediaId);
        SavorMedia media = readMysqlMedia(mediaId);
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
    public SavorHotel readMysqlHotel(String hid) throws Exception {
        if (this.hotelMap == null || this.hotelMap.get(hid) == null || this.hotelMap.size() <= 0) {
            findHotel();
        }
        return (SavorHotel) this.hotelMap.get(hid);
    }

    private void findHotel() throws SQLException {
        String sql = "select id,name from savor_hotel";
        JDBCTool jdbcUtil = new JDBCTool(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
        jdbcUtil.getConnection();
        try {
            List<SavorHotel> result = jdbcUtil.findResult(SavorHotel.class, sql);
            for (SavorHotel hotel : result) {
                this.hotelMap.put(String.valueOf(hotel.getId()), hotel);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            jdbcUtil.releaseConnection();
        }
    }

    /**
     * 查询包间信息
     *
     * @throws Exception
     */
    public SavorRoom readMysqlRoom(String rid) throws Exception {
        if (this.roomMap == null || this.roomMap.get(rid) == null || this.roomMap.size() <= 0) {
            findRoom();
        }

        return (SavorRoom) this.roomMap.get(rid);
    }

    private void findRoom() throws SQLException {
        String sql = "select id,name from savor_room";
        JDBCTool jdbcUtil = new JDBCTool(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
        jdbcUtil.getConnection();
        try {
            List<SavorRoom> result = jdbcUtil.findResult(SavorRoom.class, sql);
            for (SavorRoom room : result) {
                this.roomMap.put(String.valueOf(room.getId()), room);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            jdbcUtil.releaseConnection();
        }
    }

    /**
     * 查询机顶盒信息
     *
     * @throws Exception
     */
    public SavorBox readMysqlBox(String mac) throws Exception {
        if (this.boxMap == null || this.boxMap.get(mac) == null || this.boxMap.size() <= 0) {
            findBox();
        }

        return (SavorBox) this.boxMap.get(mac);
    }

    private void findBox() throws SQLException {
        String sql = "select id,name,mac from savor_box";
        JDBCTool jdbcUtil = new JDBCTool(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
        jdbcUtil.getConnection();
        try {
            List<SavorBox> result = jdbcUtil.findResult(SavorBox.class, sql);
            for (SavorBox box : result) {
                this.boxMap.put(String.valueOf(box.getMac()), box);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            jdbcUtil.releaseConnection();
        }
    }

    /**
     * 查询媒体信息
     *
     * @throws Exception
     */
    public SavorMedia readMysqlMedia(String mid) throws Exception {
        if (this.mediaMap == null || this.mediaMap.get(mid) == null || this.mediaMap.size() <= 0) {
            findMedia();
        }

        return (SavorMedia) this.mediaMap.get(mid);
    }

    private void findMedia() throws SQLException {
        String sql = "select id,name,oss_addr from savor_media";
        JDBCTool jdbcUtil = new JDBCTool(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
        jdbcUtil.getConnection();
        try {
            List<SavorMedia> result = jdbcUtil.findResult(SavorMedia.class, sql);
            for (SavorMedia media : result) {
                this.mediaMap.put(String.valueOf(media.getId()), media);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            jdbcUtil.releaseConnection();
        }
    }
}