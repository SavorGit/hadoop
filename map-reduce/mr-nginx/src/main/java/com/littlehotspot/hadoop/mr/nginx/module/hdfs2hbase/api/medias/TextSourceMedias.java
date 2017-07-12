package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.medias;

import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-10 下午 2:51.
 */
@Data
public class TextSourceMedias {
    /**
     * 媒体标识
     */
    private String id;

    /**
     * 媒体名称
     */
    private String name;

    /**
     * 描述信息
     */
    private String description;

    /**
     * 创建时间
     */
    private String create_time;

    /**
     * md5
     */
    private String md5;

    /**
     * 创建者标识
     */
    private String creator_id;

    /**
     * 创建者名称
     */
    private String creator_name;

    /**
     * oss相对路径
     */
    private String oss_addr;

    /**
     * 下载地址url
     */
    private String file_path;

    /**
     * 视频时长
     */
    private String duration;

    /**
     * 后缀名
     */
    private String surfix;

    /**
     * 资源类型
     * 1：视频
     * 2：图片
     * 3：其他
     */
    private String type;

    private String oss_etag;

    /**
     * 状态
     * 0：未审核,
     * 1：已审核
     */
    private String state;

    /**
     * 审核人标识
     */
    private String checker_id;

    /**
     * 审核人名称
     */
    private String checker_name;

    /**
     * 标志
     * 0：正常，
     * 1：删除
     */
    private String flag;

    public TextSourceMedias(String text) {
        List<String> group = new ArrayList<>();

        String arr[] = text.split(Constant.VALUE_SPLIT_CHAR + "");
        for (String s : arr) {
            group.add(s);
        }
        if (group.size() > 16) {
            this.setId(cleanNull(group.get(0)));
            this.setName(cleanNull(group.get(1)));
            this.setDescription(cleanNull(group.get(2)));
            this.setCreate_time(cleanNull(group.get(3)));
            this.setMd5(cleanNull(group.get(4)));
            this.setCreator_id(cleanNull(group.get(5)));
            this.setCreator_name(cleanNull(group.get(6)));
            this.setOss_addr(cleanNull(group.get(7)));
            this.setFile_path(cleanNull(group.get(8)));
            this.setDuration(cleanNull(group.get(9)));
            this.setSurfix(cleanNull(group.get(10)));
            this.setType(cleanNull(group.get(11)));
            this.setOss_etag(cleanNull(group.get(12)));
            this.setState(cleanNull(group.get(13)));
            this.setChecker_id(cleanNull(group.get(14)));
            this.setChecker_name(cleanNull(group.get(15)));
            this.setFlag(cleanValue(cleanNull(group.get(16))));
        }
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    private String cleanNull(String value) {
        if ("\\N".equals(value) || "NULL".equals(value) || "null".equals(value)) {
            return null;
        }

        return value;
    }
}
