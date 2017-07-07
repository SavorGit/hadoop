package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseTable;
import lombok.Data;
import lombok.NonNull;

/**
 * <h1> 资源基本属性 </h1>
 * Created by Administrator on 2017-07-04 下午 4:10.
 */
@Data
@HBaseTable(name = "resources")
public class TargetResourcesAttrBean {

    /**
     * 类型
     * 0x0001：内容，
     * 0x0101：广告，
     * 0x0102：宣传片，
     * 0x0103：节目
     */
    @HBaseColumn(name = "rety")
    private String resource_type;


    /**
     * 标识
     */
    @HBaseColumn(name = "id")
    private String id;

    /**
     * 内容标题、媒体名称
     */
    @HBaseColumn(name = "name")
    private String name;

    /**
     * 操作人标识
     */
    @HBaseColumn(name = "crt_id")
    private String creator_id;

    /**
     * 操作人名称
     */
    @HBaseColumn(name = "crt_name")
    private String creator_name;

    /**
     * 状态
     * 文章：
     * 0：未提交审核
     * 1：内容审核中（已提交）
     * 2：内容审核通过
     * 3：内容审核不通过
     * 视屏：
     * 0：未审核,
     * 1：已审核
     */
    @HBaseColumn(name = "state")
    private String state;

    /**
     * 标志
     * 0：正常，1：删除
     */
    @HBaseColumn(name = "flag")
    private String flag;

    /**
     * 审核人1标识
     */
    @HBaseColumn(name = "chk1_id")
    private String checker1_id;

    /**
     * 审核人1名称
     */
    @HBaseColumn(name = "chk1_name")
    private String checker1_name;

    /**
     * 时长(单位：秒)
     */
    @HBaseColumn(name = "duration")
    private String duration;

    /**
     * 创建时间
     */
    @HBaseColumn(name = "create_time")
    private String create_time;

    /**
     * 封面图片链接
     */
    @HBaseColumn(name = "img_url")
    private String img_url;

    /**
     * 媒体标识
     */
    @HBaseColumn(name = "mda_id")
    private String media_id;

    /**
     * 媒体名称
     */
    @HBaseColumn(name = "mda_name")
    private String media_name;

    /**
     * 类型
     * 文章：
     * 0：纯文本，
     * 1：图文，
     * 2：图集，
     * 3：视频
     * 媒体视屏：
     * 1：广告，
     * 2：节目，
     * 3：宣传片
     */
    @HBaseColumn(name = "type")
    private String type;

}
