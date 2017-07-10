package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 下午 3:00.
 */
@Data
public class TextSourceResources {
    /**
     * 标识
     */
    private String id;

    /**
     * 内容标题、媒体名称
     */
    private String name;

    /**
     * 操作人标识
     */
    private String creator_id;

    /**
     * 操作人名称
     */
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
    private String state;

    /**
     * 标志
     * 0：正常，1：删除
     */
    private String flag;

    /**
     * 审核人1标识
     */
    private String checker1_id;

    /**
     * 审核人1名称
     */
    private String checker1_name;

    /**
     * 时长(单位：秒)
     */
    private String duration;

    /**
     * 创建时间
     */
    private String create_time;

    /**
     * 封面图片链接
     */
    private String img_url;

    /**
     * 媒体标识
     */
    private String media_id;

    /**
     * 媒体名称
     */
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
    private String type;

    /**
     * 简介
     */
    private String introduction;

    /**
     * 分类标识
     */
    private String category_id;

    /**
     * 分类名称
     */
    private String category_name;

    /**
     * 腾讯云url
     */
    private String tx_url;

    /**
     * 正文
     */
    private String content;

    /**
     * 内容Url
     */
    private String content_url;

    /**
     * 备注
     */
    private String remark;

    /**
     * 来源
     */
    private String source;

    /**
     * 文章内容来源
     */
    private String source_id;

    /**
     * 文件大小(单位M)
     */
    private String size;

    /**
     * 审核人2标识
     */
    private String checker2_id;

    /**
     * 审核人2名称
     */
    private String checker2_name;

    /**
     * 点播视频md5值
     */
    private String vod_md5;

    /**
     * 是否预约字段
     * 0：未预约，
     * 1：已预约，
     * 2：预约正常发布(针对于进行完审核的)，
     * 3：预约过期(针对于未进行完审核的)
     */
    private String bespeak;

    /**
     * 预约时间
     */
    private String bespeak_time;

    /**
     * 更新时间
     */
    private String update_time;

    /**
     * 操作人
     */
    private String operators;

    /**
     * 分享内容
     */
    private String share_content;

    /**
     * 酒楼标识
     */
    private String hotel_id;

    /**
     * 酒楼名称
     */
    private String hotel_name;

    /**
     * 包间标识
     */
    private String room_id;

    /**
     * 包间名称
     */
    private String room_name;

    /**
     * 机顶盒标识
     */
    private String box_id;

    /**
     * 机顶盒名称
     */
    private String box_name;

    /**
     * 顺序号
     */
    private String sort_num;

    /**
     * 是否在线	1：正常2：下线
     */
    private String is_online;

    /**
     * 描述信息
     */
    private String description;

    /**
     * 标签标识
     */
    private String tag_ids;

    public TextSourceResources(String text) {
//        Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(text);
//        if (!matcher.find()) {
//            return;
//        }
        List<String> group = new ArrayList<>();

        String arr[] = text.split(Constant.VALUE_SPLIT_CHAR+"");
        for (String s : arr) {
            group.add(s);
        }
        if(group.size() > 40) {
            this.setId(cleanNull(group.get(0)));
            this.setName(cleanNull(group.get(1)));
            this.setCreator_id(cleanNull(group.get(2)));
            this.setCreator_name(cleanNull(group.get(3)));
            this.setState(cleanNull(group.get(4)));
            this.setFlag(cleanNull(group.get(5)));
            this.setChecker1_id(cleanNull(group.get(6)));
            this.setChecker1_name(cleanNull(group.get(7)));
            this.setDuration(cleanNull(group.get(8)));
            this.setCreate_time(cleanNull(group.get(9)));
            this.setImg_url(cleanNull(group.get(10)));
            this.setMedia_id(cleanNull(group.get(11)));
            this.setMedia_name(cleanNull(group.get(12)));
            this.setType(cleanNull(group.get(13)));
            this.setIntroduction(cleanNull(group.get(14)));
            this.setCategory_id(cleanNull(group.get(15)));
            this.setCategory_name(cleanNull(group.get(16)));
            this.setTx_url(cleanNull(group.get(17)));
            this.setContent(cleanNull(group.get(18)));
            this.setContent_url(cleanNull(group.get(19)));
            this.setRemark(cleanNull(group.get(20)));
            this.setSource(cleanNull(group.get(21)));
            this.setSource_id(cleanNull(group.get(22)));
            this.setSize(cleanNull(group.get(23)));
            this.setChecker2_id(cleanNull(group.get(24)));
            this.setChecker2_name(cleanNull(group.get(25)));
            this.setVod_md5(cleanNull(group.get(26)));
            this.setBespeak(cleanNull(group.get(27)));
            this.setBespeak_time(cleanNull(group.get(28)));
            this.setUpdate_time(cleanNull(group.get(29)));
            this.setOperators(cleanNull(group.get(39)));
            this.setShare_content(cleanNull(group.get(31)));
            this.setHotel_id(cleanNull(group.get(32)));
            this.setHotel_name(cleanNull(group.get(33)));
            this.setRoom_id(cleanNull(group.get(34)));
            this.setRoom_name(cleanNull(group.get(35)));
            this.setBox_id(cleanNull(group.get(36)));
            this.setBox_name(cleanNull(group.get(37)));
            this.setSort_num(cleanNull(group.get(38)));
            this.setIs_online(cleanNull(group.get(39)));
            this.setDescription(cleanNull(group.get(40)));
            this.setTag_ids(cleanValue(cleanNull(group.get(41))));
        }
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    private String cleanNull(String value){
        if("\\N".equals(value) || "NULL".equals(value) || "null".equals(value)) {
            return null;
        }

        return value;
    }
}
