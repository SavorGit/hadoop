package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources.ResourceType;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.*;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

/**
 * Created by gy on 2017/7/18.
 */
public class BoxTableReduce extends Reducer<Text, Text, Text, Text> {

    private HBaseHelper hBaseHelper;

    private Map<String, Object> hotelMap = new ConcurrentHashMap<>();

    private Map<String, Object> areaMap = new ConcurrentHashMap<>();

    private Map<String, Object> roomMap = new ConcurrentHashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.hBaseHelper = new HBaseHelper(conf);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Iterator<Text> textIterator = value.iterator();
            TargetBootRateBean bootBean = new TargetBootRateBean();
            TargetRareBean bean = new TargetRareBean();
            Integer count=0;
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                System.out.println(item.toString());
                String msg = item.toString();
                Matcher matcher = CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(msg);
                if (!matcher.find()) {
                    return;
                }
                Result medias=hBaseHelper.getOneRecord("medias", matcher.group(5));
                if (medias.isEmpty()){
                    System.out.println(matcher.group(5) + "RESULT IS EMPTY");
                    return;
                }
                    String type = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("type")));

                    if (type.equals("1")){
                        String duration = new String(medias.getValue(Bytes.toBytes("attr"), Bytes.toBytes("duration")));
                        if (StringUtils.isBlank(bean.getPlayTime())){
                            bean.setPlayTime(duration);
                        }else {
                            Long playtime =Long.valueOf(bean.getPlayTime())+Long.valueOf(duration);
                            bean.setPlayTime(playtime.toString());
                        }
                    }

                    bean.setRoomName(matcher.group(3));
                    bean.setMac(matcher.group(4));
                    bean.setPlayDate(matcher.group(6));
//                    System.out.println(medias.toString());
                    //读取mysql
                    SavorHotel hotel = readMysqlHotel(matcher.group(1));
                    bean.setHotelName(hotel.getName());
                    bean.setIsKey(Integer.toHexString(hotel.getIskey()));
                    bean.setMaintenMan(hotel.getMaintainer());
                    //读取mysql
                    SavorArea area = readMysqlArea(hotel.getArea_id().toString());
                    bean.setArea(area.getRegion_name());
                    SavorRoom room = readMysqlRoom(matcher.group(2));
                    if (room.getType()==1){
                        bean.setAddr("包间");
                    }else if(room.getType()==2){
                        bean.setAddr("大厅");
                    }else if (room.getType()==3){
                        bean.setAddr("等候区");
                    }

                count++;
            }


            double production = Double.valueOf(bean.getPlayTime())/7200;
            double v = production;
            if (v>=1.5){
                v=1.5;
            }
            DecimalFormat dcmFmt = new DecimalFormat("0.00");
            String format = dcmFmt.format(v * 100);
            bean.setProduction(format+"%");
            bean.setPlayCount(count.toString());
            bootBean.setRowKey(key.toString());
            bootBean.setTargetRareBean(bean);
            hBaseHelper.insert(bootBean);
//            context.write(new Text(bean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public SavorHotel readMysqlHotel(String hotelId) throws Exception{
        if (this.hotelMap == null || this.hotelMap.get(hotelId) == null || this.hotelMap.size() <= 0) {
            findHotel();
        }
        return (SavorHotel) this.hotelMap.get(hotelId);

    }

    private void findHotel() throws SQLException {
        String sql = "select id,name,area_id,iskey,maintainer from savor_hotel";
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

    public SavorArea readMysqlArea(String areaId) throws Exception{
        if (this.areaMap == null || this.areaMap.get(areaId) == null || this.areaMap.size() <= 0) {
            findArea();
        }
        return (SavorArea) this.areaMap.get(areaId);

    }

    private void findArea() throws SQLException {
        String sql = "select id,region_name from savor_area_info";
        JDBCTool jdbcUtil = new JDBCTool(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
        jdbcUtil.getConnection();
        try {
            List<SavorArea> result = jdbcUtil.findResult(SavorArea.class, sql);
            for (SavorArea area : result) {
                this.areaMap.put(String.valueOf(area.getId()), area);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            jdbcUtil.releaseConnection();
        }
    }

    public SavorRoom readMysqlRoom(String roomId) throws Exception{
        if (this.roomMap == null || this.roomMap.get(roomId) == null || this.roomMap.size() <= 0) {
            findRoom();
        }
        return (SavorRoom) this.roomMap.get(roomId);

    }

    private void findRoom() throws SQLException {
        String sql = "select id,name,type from savor_room";
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
}
