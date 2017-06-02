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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user;

import org.apache.commons.lang.StringUtils;
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
public class UserReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Iterator<Text> textIterator = value.iterator();
            TextTargetUserBean targetUserBean = new TextTargetUserBean();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                String rowLineContent = item.toString();
                TextSourceUserBean sourceUserBean = new TextSourceUserBean(rowLineContent);
                targetUserBean.setDeviceId(sourceUserBean.getDeviceId());
                targetUserBean.setDeviceType(sourceUserBean.getDeviceType());
                targetUserBean.setMachineModel(sourceUserBean.getMachineModel());
                targetUserBean.setSince(sourceUserBean.getChannelId());
//                targetUserBean.setToken();
//                targetUserBean.setDemandTime();
//                targetUserBean.setProjectionTime();
                this.setDownloadTime(sourceUserBean, targetUserBean);
            }
            context.write(new Text(targetUserBean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setDownloadTime(TextSourceUserBean sourceUserBean, TextTargetUserBean targetUserBean) {
        if (sourceUserBean == null) {
            throw new IllegalArgumentException("Argument 'sourceUserBean' is null");
        }
        if (targetUserBean == null) {
            throw new IllegalArgumentException("Argument 'targetUserBean' is null");
        }
        String timestamp = sourceUserBean.getTimestamp();
        if (timestamp == null) {
            return;
        }
        if (!timestamp.matches("^\\d+$")) {
            return;
        }
        timestamp = timestamp.trim();
        if (StringUtils.isBlank(targetUserBean.getDownloadTime())) {
            targetUserBean.setDownloadTime(timestamp);
        } else if (targetUserBean.getDownloadTime().compareTo(timestamp) > 0) {
            targetUserBean.setDownloadTime(timestamp);
        }
    }
}
