package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-06 下午 4:02.
 */
public class ResourceReducer extends Reducer<Text, Text, Text, Text> {

    private HBaseHelper hBaseHelper;
    private ResourceType resourceType;

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {

            Resources targetResource = new Resources();

            Iterator<Text> textIterator = value.iterator();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }

                String rowLineContent = item.toString();
                TextSourceResources source = new TextSourceResources(rowLineContent);

                targetResource.setRowKey(source.getId() + resourceType.getValue());
                targetResource.setAttrBean(setAttr(source, resourceType));
                targetResource.setAdatBean(setAdat(source));
            }

            this.hBaseHelper.insert(targetResource);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.hBaseHelper = new HBaseHelper(conf);
        this.resourceType = ResourceType.valueOf(conf.get("resourceType"));
    }

    private TargetResourcesAttrBean setAttr(TextSourceResources source, ResourceType resourceType) {

        TargetResourcesAttrBean attrBean = new TargetResourcesAttrBean();
        if (resourceType != null && ResourceType.CON.equals(resourceType)) {
            attrBean.setResource_type(resourceType.getValue());
        } else {
            switch (source.getType()) {
                case "1":
                    attrBean.setResource_type(ResourceType.ADS.getValue());
                    break;
                case "2":
                    attrBean.setResource_type(ResourceType.PRO.getValue());
                    break;
                case "3":
                    attrBean.setResource_type(ResourceType.ADV.getValue());
                    break;
            }
        }
        attrBean.setId(source.getId());
        attrBean.setName(source.getName());
        attrBean.setCreator_id(source.getCreator_id());
        attrBean.setCreator_name(source.getCreator_name());
        attrBean.setState(source.getState());
        attrBean.setFlag(source.getFlag());
        attrBean.setChecker1_id(source.getChecker1_id());
        attrBean.setChecker1_name(source.getChecker1_name());
        attrBean.setDuration(source.getDuration());
        attrBean.setCreate_time(source.getCreate_time());
        attrBean.setImg_url(source.getImg_url());
        attrBean.setMedia_id(source.getMedia_id());
        attrBean.setMedia_name(source.getMedia_name());
        attrBean.setType(source.getType());

        return attrBean;

    }

    private TargetResourcesAdatBean setAdat(TextSourceResources source) {

        TargetResourcesAdatBean adatBean = new TargetResourcesAdatBean();
        adatBean.setIntroduction(source.getIntroduction());
        adatBean.setCategory_id(source.getCategory_id());
        adatBean.setCategory_name(source.getCategory_name());
        adatBean.setTx_url(source.getTx_url());
        adatBean.setContent(source.getContent());
        adatBean.setContent_url(source.getContent_url());
        adatBean.setRemark(source.getRemark());
        adatBean.setSource(source.getSource());
        adatBean.setSource_id(source.getSource_id());
        adatBean.setSize(source.getSize());
        adatBean.setChecker2_id(source.getChecker2_id());
        adatBean.setChecker2_name(source.getChecker2_name());
        adatBean.setVod_md5(source.getVod_md5());
        adatBean.setBespeak(source.getBespeak());
        adatBean.setBespeak_time(source.getBespeak_time());
        adatBean.setUpdate_time(source.getUpdate_time());
        adatBean.setOperators(source.getOperators());
        adatBean.setShare_content(source.getShare_content());
        adatBean.setHotel_id(source.getHotel_id());
        adatBean.setHotel_name(source.getHotel_name());
        adatBean.setRoom_id(source.getRoom_id());
        adatBean.setRoom_name(source.getRoom_name());
        adatBean.setBox_id(source.getBox_id());
        adatBean.setBox_name(source.getBox_name());
        adatBean.setSort_num(source.getSort_num());
        adatBean.setIs_online(source.getIs_online());
        adatBean.setDescription(source.getDescription());
        adatBean.setTag_ids(source.getTag_ids());

        return adatBean;

    }
}
