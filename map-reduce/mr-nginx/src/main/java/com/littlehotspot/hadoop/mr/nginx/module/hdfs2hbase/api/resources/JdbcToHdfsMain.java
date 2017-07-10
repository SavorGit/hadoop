package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.mysql.JdbcReader;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.HdfsStringModel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SelectModel;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 下午 6:00.
 */
public class JdbcToHdfsMain {

    public static void main(String[] args) {
        String sql;
        if(args != null && args.length > 0){
            sql = args[0];
        }else{
            sql = "SELECT\n" +
                "\tcon.id,\n" +
                "\tcon.title AS NAME,\n" +
                "\tcon.creator_id,\n" +
                "\tuser.username AS creator_name,\n" +
                "\tcon.state,\n" +
                "\tNULL AS flag,\n" +
                "\tcon.cheaker1_id AS checker1_id,\n" +
                "\tchck1.username AS checker1_name,\n" +
                "\tcon.duration,\n" +
                "\tcon.create_time,\n" +
                "\tcon.img_url,\n" +
                "\tcon.media_id,\n" +
                "\tmedia.name AS media_name,\n" +
                "\tcon.type,\n" +
                "\tcon.share_title AS introduction,\n" +
                "\tcon.category_id,\n" +
                "\tcate.name AS category_name,\n" +
                "\tcon.tx_url,\n" +
                "\tcon.content,\n" +
                "\tcon.content_url,\n" +
                "\tcon.remark,\n" +
                "\tcon.source,\n" +
                "\tNULL AS source_id,\n" +
                "\tcon.size,\n" +
                "\tcon.cheaker2_id AS checker2_id,\n" +
                "\tchck2.username AS checker2_name,\n" +
                "\tcon.vod_md5,\n" +
                "\tcon.bespeak,\n" +
                "\tcon.bespeak_time,\n" +
                "\tcon.update_time,\n" +
                "\tcon.operators,\n" +
                "\tcon.share_content,\n" +
                "\tNULL AS hotel_id,\n" +
                "\tNULL AS hotel_name,\n" +
                "\tNULL AS room_id,\n" +
                "\tNULL AS room_name,\n" +
                "\tNULL AS box_id,\n" +
                "\tNULL AS box_name,\n" +
                "\tNULL AS sort_num,\n" +
                "\tNULL AS is_online,\n" +
                "\tNULL AS description,\n" +
                "\tNULL AS tag_ids\n" +
                "FROM\n" +
                "\tsavor_mb_content con\n" +
                "LEFT JOIN savor_sysuser user ON con.creator_id=user.id\n" +
                "LEFT JOIN savor_sysuser chck1 ON con.cheaker1_id=`user`.id\n" +
                "LEFT JOIN savor_sysuser chck2 ON con.cheaker2_id=`user`.id\n" +
                "LEFT JOIN savor_media media ON con.media_id=media.id\n" +
                "LEFT JOIN savor_mb_category cate ON con.category_id=cate.id";
        }

        try {
            SelectModel selectModel = new SelectModel();
            selectModel.setInputClass(HdfsStringModel.class);

            selectModel.setQuery(sql);
            selectModel.setCountQuery("select count(*) from savor_mb_content");

            selectModel.setOutputPath("/home/data/hadoop/flume/test_hbase/mysql");

            JdbcReader.readToHdfs("hdfs://devpd1:8020", selectModel);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
