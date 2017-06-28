package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.mysql.service.BoxService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.HotelService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.MediaService;
import com.littlehotspot.hadoop.mr.nginx.mysql.service.RoomService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <h1> 用户行为最终处理并插入到 Hbase </h1>
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

            if(targetSadAttrBean.getStart() != 0 && targetSadAttrBean.getEnd() != 0) {
                CommonVariables.hBaseHelper.insert(targetSadAttrBean);
                CommonVariables.hBaseHelper.insert(targetSadRelaBean);
            }

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
        HotelService hotelService = new HotelService();
        bean.setHotel_name(hotelService.getName(source.getHotel_id()));

        bean.setRoom(source.getRoom_id());
        RoomService roomService = new RoomService();
        bean.setRoom_name(roomService.getName(source.getRoom_id()));

        bean.setBox_mac(source.getMac());
        BoxService boxService = new BoxService();
        bean.setBox_name(boxService.getName(source.getMac()));

        bean.setMedia(source.getMedia_id());
        MediaService mediaService = new MediaService();
        bean.setMedia_name(mediaService.getName(source.getMedia_id()));
        bean.setMedia_down_url(mediaService.getUrl(source.getMedia_id()));

        bean.setApk_version(source.getApk_version());
        bean.setAds_version(source.getAds_period());
        bean.setDema_version(source.getDemand_period());
    }
}