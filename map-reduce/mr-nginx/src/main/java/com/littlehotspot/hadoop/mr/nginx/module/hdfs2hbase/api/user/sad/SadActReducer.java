package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <h1> 用户行为reducer </h1>
 * Created by Administrator on 2017-06-26 下午 5:56.
 */
public class SadActReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            SadActType sadType = SadActType.valueOf(conf.get("sadActType"));

            TextTargetSadActBean targetUserSadActBean = TargetBeanFactory.getTargetActBean(sadType);

            Iterator<Text> textIterator = value.iterator();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                String rowLineContent = item.toString();
                TextSourceUserSadBean sourceUserSadBean = new TextSourceUserSadBean(rowLineContent);
                targetUserSadActBean.setRowKey(sourceUserSadBean.getUuid()+sourceUserSadBean.getMedia_id());
                targetUserSadActBean.setUuid(sourceUserSadBean.getUuid());
                targetUserSadActBean.setHotel_id(sourceUserSadBean.getHotel_id());
                targetUserSadActBean.setRoom_id(sourceUserSadBean.getRoom_id());
                targetUserSadActBean.setTimestamps(sourceUserSadBean.getTimestamps());
                targetUserSadActBean.setOption_type(sourceUserSadBean.getOption_type());
                targetUserSadActBean.setMedia_type(sourceUserSadBean.getMedia_type());
                targetUserSadActBean.setMedia_id(sourceUserSadBean.getMedia_id());
                targetUserSadActBean.setMobile_id(sourceUserSadBean.getMobile_id());
                targetUserSadActBean.setApk_version(sourceUserSadBean.getApk_version());
                targetUserSadActBean.setAds_period(sourceUserSadBean.getAds_period());
                targetUserSadActBean.setDemand_period(sourceUserSadBean.getDemand_period());
                targetUserSadActBean.setCommon_value(sourceUserSadBean.getCommon_value());
                targetUserSadActBean.setMac(sourceUserSadBean.getMac());
            }

//            CommonVariables.hBaseHelper.insert(targetUserSadActBean);

            context.write(new Text(targetUserSadActBean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}