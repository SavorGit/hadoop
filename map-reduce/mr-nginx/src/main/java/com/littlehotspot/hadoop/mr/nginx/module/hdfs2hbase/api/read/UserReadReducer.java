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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.read;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <h1>Reducer - 用户</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class UserReadReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        try {
            Iterator<Text> textIterator = value.iterator();
            TargetUserReadBean targetUserReadBean = new TargetUserReadBean();
            TargetUserReadAttrBean targetReadBean = new TargetUserReadAttrBean();
            TargetUserReadRelaBean targetReadRelaBean = new TargetUserReadRelaBean();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                String rowLineContent = item.toString();
                SourceUserReadBean sourceReadBean = new SourceUserReadBean(rowLineContent);
                StringBuffer rowLine = new StringBuffer();
                targetUserReadBean.setRowKey(sourceReadBean.getMobileId()+sourceReadBean.getStartTime().substring(0,10));
                targetReadBean.setDeviceId(sourceReadBean.getMobileId());
                targetReadBean.setStart(sourceReadBean.getStartTime());
                targetReadBean.setEnd(sourceReadBean.getEndTime());
                targetReadBean.setConId(sourceReadBean.getContentId());
                targetReadBean.setConNam(sourceReadBean.getTitle());
                targetReadBean.setVTime(sourceReadBean.getDuration());
                targetReadBean.setLongitude(sourceReadBean.getLongitude());
                targetReadBean.setLatitude(sourceReadBean.getLatitude());
                targetReadBean.setOsType(sourceReadBean.getOsType());

                targetReadRelaBean.setDeviceId(sourceReadBean.getMobileId());
                targetReadRelaBean.setStart(sourceReadBean.getStartTime());
                targetReadRelaBean.setCatId(sourceReadBean.getCategoryId());
                targetReadRelaBean.setCatName(sourceReadBean.getCategoryName());
                targetReadRelaBean.setHotel(sourceReadBean.getHotelId());
                targetReadRelaBean.setHotelName(sourceReadBean.getHotelName());
                targetReadRelaBean.setRoom(sourceReadBean.getRoomId());
                targetReadRelaBean.setRoomName(sourceReadBean.getRoomName());

                this.setDownloadTime(sourceReadBean, targetReadBean);
            }

            targetUserReadBean.setTargetUserReadAttrBean(targetReadBean);
            targetUserReadBean.setTargetUserReadRelaBean(targetReadRelaBean);
            CommonVariables.hBaseHelper.insert(targetReadBean);

            CommonVariables.hBaseHelper.insert(targetReadRelaBean);

            context.write(new Text(targetReadBean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setDownloadTime(SourceUserReadBean sourceUserBean, TargetUserReadAttrBean targetUserBean) {
        if (sourceUserBean == null) {
            throw new IllegalArgumentException("Argument 'sourceUserBean' is null");
        }
        if (targetUserBean == null) {
            throw new IllegalArgumentException("Argument 'targetUserBean' is null");
        }
//        String timestamp = sourceUserBean.getTimestamp();
//        if (timestamp == null) {
//            return;
//        }
//        if (!timestamp.matches("^\\d+$")) {
//            return;
//        }
//        timestamp = timestamp.trim();
//        if (StringUtils.isBlank(targetUserBean.getDownloadTime())) {
//            targetUserBean.setDownloadTime(timestamp);
//        } else if (targetUserBean.getDownloadTime().compareTo(timestamp) > 0) {
//            targetUserBean.setDownloadTime(timestamp);
//        }
    }
}
