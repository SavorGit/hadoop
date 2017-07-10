package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources.CommonVariables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-10 下午 6:12.
 */
public class MediaReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        try {

            Medias targetResource = new Medias();

            Iterator<Text> textIterator = value.iterator();
            while (textIterator.hasNext()) {
                Text item = textIterator.next();
                if (item == null) {
                    continue;
                }

                String rowLineContent = item.toString();
                TextSourceMedias source = new TextSourceMedias(rowLineContent);

                targetResource.setRowKey(source.getId());
                targetResource.setAttrBean(setAttr(source));

            }

            CommonVariables.hBaseHelper.insert(targetResource);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private TargetMediasAttrBean setAttr(TextSourceMedias source) {

        TargetMediasAttrBean attrBean = new TargetMediasAttrBean();

        attrBean.setId(source.getId());
        attrBean.setName(source.getName());
        attrBean.setDescription(source.getDescription());
        attrBean.setCreate_time(source.getCreate_time());
        attrBean.setMd5(source.getMd5());
        attrBean.setCreator_id(source.getCreator_id());
        attrBean.setCreator_name(source.getCreator_name());
        attrBean.setOss_addr(source.getOss_addr());
        attrBean.setFile_path(source.getFile_path());
        attrBean.setDuration(source.getDuration());
        attrBean.setSurfix(source.getSurfix());
        attrBean.setType(source.getType());
        attrBean.setOss_etag(source.getOss_etag());
        attrBean.setState(source.getState());
        attrBean.setChecker_id(source.getChecker_id());
        attrBean.setChecker_name(source.getChecker_name());
        attrBean.setFlag(source.getFlag());

        return attrBean;
    }

}
