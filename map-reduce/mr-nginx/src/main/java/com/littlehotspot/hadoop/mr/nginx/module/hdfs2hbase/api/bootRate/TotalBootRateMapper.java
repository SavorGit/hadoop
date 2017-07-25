package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.bootRate;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by gy on 2017/7/18.
 */
public class TotalBootRateMapper extends TableMapper<Text, Text> {


    @Override
    protected void map(ImmutableBytesWritable rowKey, Result result, Context context){
//        System.out.println(result.toString());
        String row = Bytes.toString(result.getRow());
        String area = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("area")));
        String hotelName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_name")));
        String addr = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("addr")));
        String mac = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac")));
        String roomName = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_name")));
        String maintenMan = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mainten_man")));
        String isKey = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("isKey")));
        String playCount = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_count")));
        String playTime = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("play_time")));
        String production = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("production")));
        TargetRareBean bean = new TargetRareBean();
        bean.setArea(area);
        bean.setHotelName(hotelName);
        bean.setAddr(addr);
        bean.setMac(mac);
        bean.setRoomName(roomName);
        bean.setMaintenMan(maintenMan);
        bean.setIsKey(isKey);
        bean.setPlayCount(playCount);
        bean.setPlayTime(playTime);
        bean.setProduction(production);

//        System.out.println("ROWKEY{"+row+"}"+":mda_type="+mediaType+":option_type="+optionType+":mda_id="+mediaId);
        try {
            if (!StringUtils.isBlank(mac)&&(Double.valueOf(production)-0.1>=0)){
                context.write(new Text(mac), new Text(bean.rowLine()));
            }else {
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }
}
