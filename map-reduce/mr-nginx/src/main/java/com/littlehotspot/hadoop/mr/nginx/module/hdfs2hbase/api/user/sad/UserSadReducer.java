package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-27 下午 3:05.
 */
public class UserSadReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {
            Configuration conf = context.getConfiguration();
            SadType sadType = SadType.valueOf(conf.get("sadType"));

            TextTargetSadAttrBean targetSadAttrBean = TargetBeanFactory.getTargetSadAttrBean(sadType);
            TextTargetSadRelaBean targetSadRelaBean = TargetBeanFactory.getTargetSadRelaBean(sadType);

            Iterator<Text> textIterator = value.iterator();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }
                String rowLineContent = item.toString();
                TextTargetSadActBean sourceUserSadBean = new TextTargetSadActBean(rowLineContent);

                this.setPropertiesForAttrBean(targetSadAttrBean, sourceUserSadBean);
                this.setPropertiesForRelaBean(targetSadRelaBean, sourceUserSadBean);

            }

            CommonVariables.hBaseHelper.insert(targetSadAttrBean);
            CommonVariables.hBaseHelper.insert(targetSadRelaBean);

//            context.write(new Text(targetUserSadActBean.rowLine()), new Text());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void setPropertiesForAttrBean(TextTargetSadAttrBean bean, TextTargetSadActBean source) {
        bean.setRowKey(source.getMobile_id() + source.getTimestamps());
        bean.setDevice_id(source.getMobile_id());
        bean.setType(source.getMedia_type());
        if ("start".equals(source.getOption_type())) {
            bean.setStart(Long.decode(source.getTimestamps().substring(0, 10)));
        } else if ("end".equals(source.getOption_type())) {
            bean.setEnd(Long.decode(source.getTimestamps().substring(0, 10)));
        }
    }

    private void setPropertiesForRelaBean(TextTargetSadRelaBean bean, TextTargetSadActBean source) {
        bean.setRowKey(source.getMobile_id() + source.getTimestamps());
        bean.setHotel(source.getHotel_id());
        bean.setHotel_name("");
        bean.setRoom(source.getRoom_id());
        bean.setRoom_name("");
        bean.setBox_mac(source.getMac());
        bean.setBox_name("");
        bean.setMedia(source.getMedia_id());
        bean.setMedia_name("");
        bean.setMedia_down_url("");
        bean.setApk_version(source.getApk_version());
        bean.setAds_version(source.getAds_period());
        bean.setDema_version(source.getDemand_period());
    }
}