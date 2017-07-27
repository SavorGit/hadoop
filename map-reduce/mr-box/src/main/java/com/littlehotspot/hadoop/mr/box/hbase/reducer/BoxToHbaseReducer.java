/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 18:31
 */
package com.littlehotspot.hadoop.mr.box.hbase.reducer;

import com.littlehotspot.hadoop.mr.box.hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.box.hbase.model.SourceBoxBean;
import com.littlehotspot.hadoop.mr.box.hbase.model.TargetBoxAttrBean;
import com.littlehotspot.hadoop.mr.box.hbase.model.TargetBoxBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 整合数据reducer
 */
public class BoxToHbaseReducer extends Reducer<Text, Text, Text, Text> {
    /**
     * rowkey 分隔符
     */
    private HBaseHelper hBaseHelper;
    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        try {
            String rowLineContent = key.toString();
            SourceBoxBean boxBean=new SourceBoxBean(rowLineContent);
            //属性
            TargetBoxAttrBean attrBean=new TargetBoxAttrBean();
            attrBean.setUuid(boxBean.getUuid());
            attrBean.setHotelId(boxBean.getHotelId());
            attrBean.setHotelName(boxBean.getHotelName());
            attrBean.setRoomId(boxBean.getRoomId());
            attrBean.setRoomName(boxBean.getRoomName());
            attrBean.setMac(boxBean.getMac());
            attrBean.setTimestamps(boxBean.getTimestamps());
            attrBean.setOptionType(boxBean.getOptionType());
            attrBean.setMdaId(boxBean.getMdaId());
            attrBean.setMdaType(boxBean.getMdaType());
            attrBean.setMediaName(boxBean.getMediaName());
            attrBean.setMobileId(boxBean.getMobileId());
            attrBean.setApkVersion(boxBean.getApkVersion());
            attrBean.setAdsPeriod(boxBean.getAdsPeriod());
            attrBean.setDmdPeriod(boxBean.getDmdPeriod());
            attrBean.setDateTime(boxBean.getDateTime());
            attrBean.setCustomVolume(boxBean.getCustomVolume());

            TargetBoxBean targetBoxBean=new TargetBoxBean();
            Long timestr=9999999999999L-Long.parseLong(attrBean.getTimestamps());
            String rowKey=attrBean.getMac()+"|"+attrBean.getMdaType()+"|"+attrBean.getOptionType()+"|"+timestr;
            targetBoxBean.setRowKey(rowKey);
            targetBoxBean.setAttrBean(attrBean);

            hBaseHelper.insert(targetBoxBean);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.hBaseHelper = new HBaseHelper(conf);
    }

    @Override
    protected void cleanup(Context context){
        this.hBaseHelper.closeConnection();
    }
}
