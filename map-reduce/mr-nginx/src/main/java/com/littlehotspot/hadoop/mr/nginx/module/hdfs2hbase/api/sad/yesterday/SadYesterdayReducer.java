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
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.yesterday;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.CommonVariables;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad.SourceSadBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <h1>Reducer - 投屏点播</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class SadYesterdayReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
        try {
            Iterator<Text> textIterator = value.iterator();
            TargetSadYesterdayBean targetSadBean = new TargetSadYesterdayBean();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                String rowLineContent = item.toString();
                SourceSadBean sourceSadBean = new SourceSadBean(rowLineContent);
                targetSadBean.setAreaId(sourceSadBean.getAreaId());
                targetSadBean.setAreaName(sourceSadBean.getAreaName());
                targetSadBean.setHotelId(sourceSadBean.getHotelId());
                targetSadBean.setHotelName(sourceSadBean.getHotelName());
                targetSadBean.setRoomId(sourceSadBean.getRoomId());
                targetSadBean.setRoomName(sourceSadBean.getRoomName());
                targetSadBean.setBoxId(sourceSadBean.getBoxId());
                targetSadBean.setBoxName(sourceSadBean.getBoxName());
                targetSadBean.setBoxMac(sourceSadBean.getBoxMac());
                targetSadBean.setMobileId(sourceSadBean.getMobileId());
                targetSadBean.setProjectCount(sourceSadBean.getProjectCount());
                targetSadBean.setDemandCount(sourceSadBean.getDemandCount());
                targetSadBean.setTime(sourceSadBean.getTime());
            }

            CommonVariables.hBaseHelper.insert(targetSadBean);

            context.write(new Text(targetSadBean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
