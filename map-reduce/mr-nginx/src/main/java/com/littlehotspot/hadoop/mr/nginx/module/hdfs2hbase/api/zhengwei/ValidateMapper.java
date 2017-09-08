package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.zhengwei;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by gy on 2017/7/18.
 */
public class ValidateMapper extends TableMapper<Text, Text> {

    private SimpleDateFormat format1;
    private SimpleDateFormat format2;
    private SimpleDateFormat format3;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        format1=new SimpleDateFormat("yyyyMMddHH");
        format2=new SimpleDateFormat("yyyy/MM/dd");
        format3=new SimpleDateFormat("HH:mm:ss");
    }


    @Override
    protected void map(ImmutableBytesWritable rowKey, Result result, Context context){
//        System.out.println(result.toString());
        String row = Bytes.toString(result.getRow());
        String mediaId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_id")));
        String mediaType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mda_type")));
        String optionType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type")));
        String mac = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("mac")));
        String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
        String hotelId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("hotel_id")));
        String roomId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("room_id")));
        SourceRareBean sourceRareBean = new SourceRareBean();
        sourceRareBean.setMediaId(mediaId);
        sourceRareBean.setHotelId(hotelId);
        sourceRareBean.setRoomId(roomId);
        sourceRareBean.setMac(mac);
        sourceRareBean.setPlayDate(stampToDate(timestamps,format1));
        sourceRareBean.setDate(stampToDate(timestamps,format2));
        sourceRareBean.setTime(stampToDate(timestamps,format3));
        sourceRareBean.setMediaType(mediaType);
        sourceRareBean.setOptionType(optionType);
//        System.out.println("ROWKEY{"+row+"}"+":mda_type="+mediaType+":option_type="+optionType+":mda_id="+mediaId);
        try {
            if (!StringUtils.isBlank(mediaId)){
                context.write(new Text(row), new Text(sourceRareBean.rowLine1()));
            }else {
                return;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static String stampToDate(String s,SimpleDateFormat format){
        String res;
        long lt = Long.valueOf(s);
        Date date = new Date(lt);
        res = format.format(date);
        return res;
    }

}
