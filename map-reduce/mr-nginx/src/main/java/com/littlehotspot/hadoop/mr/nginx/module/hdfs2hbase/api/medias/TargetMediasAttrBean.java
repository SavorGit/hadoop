package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import lombok.Data;

/**
 * <h1> 媒体表基本属性 </h1>
 * Created by Administrator on 2017-07-10 下午 2:37.
 */
@Data
class TargetMediasAttrBean {

    /**
     * 媒体标识
     */
    @HBaseColumn(name = "id")
    private String id;

    /**
     * 媒体名称
     */
    @HBaseColumn(name = "name")
    private String name;

    /**
     * 描述信息
     */
    @HBaseColumn(name = "description")
    private String description;

    /**
     * 创建时间
     */
    @HBaseColumn(name = "create_time")
    private String create_time;

    /**
     * md5
     */
    @HBaseColumn(name = "md5")
    private String md5;

    /**
     * 创建者标识
     */
    @HBaseColumn(name = "crt_id")
    private String creator_id;

    /**
     * 创建者名称
     */
    @HBaseColumn(name = "crt_name")
    private String creator_name;

    /**
     * oss相对路径
     */
    @HBaseColumn(name = "oss_addr")
    private String oss_addr;

    /**
     * 下载地址url
     */
    @HBaseColumn(name = "file_path")
    private String file_path;

    /**
     * 视频时长
     */
    @HBaseColumn(name = "duration")
    private String duration;

    /**
     * 后缀名
     */
    @HBaseColumn(name = "surfix")
    private String surfix;

    /**
     * 资源类型
     *  1：视频
     *  2：图片
     *  3：其他
     */
    @HBaseColumn(name = "type")
    private String type;

    @HBaseColumn(name = "oss_etag")
    private String oss_etag;

    /**
     * 状态
     *  0：未审核,
     *  1：已审核
     */
    @HBaseColumn(name = "state")
    private String state;

    /**
     * 审核人标识
     */
    @HBaseColumn(name = "chk_id")
    private String checker_id;

    /**
     * 审核人名称
     */
    @HBaseColumn(name = "chk_name")
    private String checker_name;

    /**
     * 标志
     *  0：正常，
     *  1：删除
     */
    @HBaseColumn(name = "flag")
    private String flag;

}
