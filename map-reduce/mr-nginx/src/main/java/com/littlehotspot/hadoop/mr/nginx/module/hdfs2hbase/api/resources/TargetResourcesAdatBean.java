package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;
import lombok.NonNull;

/**
 * <h1> 资源附加属性 </h1>
 * Created by Administrator on 2017-07-04 下午 4:38.
 */
@Data
@HBaseTable(name = "resources")
public class TargetResourcesAdatBean {


    /**
     * 简介
     */
    @HBaseColumn(name = "introduction")
    private String introduction;

    /**
     * 分类标识
     */
    @HBaseColumn(name = "caty_id")
    private String category_id;

    /**
     * 分类名称
     */
    @HBaseColumn(name = "caty_name")
    private String category_name;

    /**
     * 腾讯云url
     */
    @HBaseColumn(name = "tx_url")
    private String tx_url;

    /**
     * 正文
     */
    @HBaseColumn(name = "content")
    private String content;

    /**
     * 内容Url
     */
    @HBaseColumn(name = "content_url")
    private String content_url;

    /**
     * 备注
     */
    @HBaseColumn(name = "remark")
    private String remark;

    /**
     * 来源
     */
    @HBaseColumn(name = "source")
    private String source;

    /**
     * 文章内容来源
     */
    @HBaseColumn(name = "source_id")
    private String source_id;

    /**
     * 文件大小(单位M)
     */
    @HBaseColumn(name = "size")
    private String size;

    /**
     * 审核人2标识
     */
    @HBaseColumn(name = "chk2_id")
    private String checker2_id;

    /**
     * 审核人2名称
     */
    @HBaseColumn(name = "chk2_name")
    private String checker2_name;

    /**
     * 点播视频md5值
     */
    @HBaseColumn(name = "vod_md5")
    private String vod_md5;

    /**
     * 是否预约字段
     *  0：未预约，
     *  1：已预约，
     *  2：预约正常发布(针对于进行完审核的)，
     *  3：预约过期(针对于未进行完审核的)
     */
    @HBaseColumn(name = "bespk")
    private String bespeak;

    /**
     * 预约时间
     */
    @HBaseColumn(name = "bespk_time")
    private String bespeak_time;

    /**
     * 更新时间
     */
    @HBaseColumn(name = "update_time")
    private String update_time;

    /**
     * 操作人
     */
    @HBaseColumn(name = "operators")
    private String operators;

    /**
     * 分享内容
     */
    @HBaseColumn(name = "share_cont")
    private String share_content;

    /**
     * 酒楼标识
     */
    @HBaseColumn(name = "hotel_id")
    private String hotel_id;

    /**
     * 酒楼名称
     */
    @HBaseColumn(name = "hotel_name")
    private String hotel_name;

    /**
     * 包间标识
     */
    @HBaseColumn(name = "room_id")
    private String room_id;

    /**
     * 包间名称
     */
    @HBaseColumn(name = "room_name")
    private String room_name;

    /**
     * 机顶盒标识
     */
    @HBaseColumn(name = "box_id")
    private String  box_id;

    /**
     * 机顶盒名称
     */
    @HBaseColumn(name = "box_name")
    private String box_name;

    /**
     * 顺序号
     */
    @HBaseColumn(name = "sort_num")
    private String sort_num;

    /**
     * 是否在线	1：正常2：下线
     */
    @HBaseColumn(name = "is_online")
    private String is_online;

    /**
     * 描述信息
     */
    @HBaseColumn(name = "description")
    private String description;

    /**
     * 标签标识
     */
    @HBaseColumn(name = "tag_ids")
    private String tag_ids;

}
