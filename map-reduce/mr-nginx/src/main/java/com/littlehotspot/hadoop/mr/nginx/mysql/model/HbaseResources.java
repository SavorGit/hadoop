package com.littlehotspot.hadoop.mr.nginx.mysql.model;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad.CommonVariables;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-05 上午 11:00.
 */
@Data
public class HbaseResources extends Model {

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

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, this.id);
        Text.writeString(out, this.name);
        Text.writeString(out, this.creator_id);
        Text.writeString(out, this.creator_name);
        Text.writeString(out, this.state);
        Text.writeString(out, this.flag);
        Text.writeString(out, this.checker1_id);
        Text.writeString(out, this.checker1_name);
        Text.writeString(out, this.duration);
        Text.writeString(out, this.create_time);
        Text.writeString(out, this.img_url);
        Text.writeString(out, this.media_id);
        Text.writeString(out, this.media_name);
        Text.writeString(out, this.type);
        Text.writeString(out, this.introduction);
        Text.writeString(out, this.category_id);
        Text.writeString(out, this.category_name);
        Text.writeString(out, this.tx_url);
        Text.writeString(out, this.content);
        Text.writeString(out, this.content_url);
        Text.writeString(out, this.remark);
        Text.writeString(out, this.source);
        Text.writeString(out, this.source_id);
        Text.writeString(out, this.size);
        Text.writeString(out, this.checker2_id);
        Text.writeString(out, this.checker2_name);
        Text.writeString(out, this.vod_md5);
        Text.writeString(out, this.bespeak);
        Text.writeString(out, this.bespeak_time);
        Text.writeString(out, this.update_time);
        Text.writeString(out, this.operators);
        Text.writeString(out, this.share_content);
        Text.writeString(out, this.hotel_id);
        Text.writeString(out, this.hotel_name);
        Text.writeString(out, this.room_id);
        Text.writeString(out, this.room_name);
        Text.writeString(out, this.box_id);
        Text.writeString(out, this.box_name);
        Text.writeString(out, this.sort_num);
        Text.writeString(out, this.is_online);
        Text.writeString(out, this.description);
        Text.writeString(out, this.tag_ids);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.creator_id = in.readUTF();
        this.creator_name = in.readUTF();
        this.state = in.readUTF();
        this.flag = in.readUTF();
        this.checker1_id = in.readUTF();
        this.checker1_name = in.readUTF();
        this.duration = in.readUTF();
        this.create_time = in.readUTF();
        this.img_url = in.readUTF();
        this.media_id = in.readUTF();
        this.media_name = in.readUTF();
        this.type = in.readUTF();
        this.introduction = in.readUTF();
        this.category_id = in.readUTF();
        this.category_name = in.readUTF();
        this.tx_url = in.readUTF();
        this.content = in.readUTF();
        this.content_url = in.readUTF();
        this.remark = in.readUTF();
        this.source = in.readUTF();
        this.source_id = in.readUTF();
        this.size = in.readUTF();
        this.checker2_id = in.readUTF();
        this.checker2_name = in.readUTF();
        this.vod_md5 = in.readUTF();
        this.bespeak = in.readUTF();
        this.bespeak_time = in.readUTF();
        this.update_time = in.readUTF();
        this.operators = in.readUTF();
        this.share_content = in.readUTF();
        this.hotel_id = in.readUTF();
        this.hotel_name = in.readUTF();
        this.room_id = in.readUTF();
        this.room_name = in.readUTF();
        this.box_id = in.readUTF();
        this.box_name = in.readUTF();
        this.sort_num = in.readUTF();
        this.is_online = in.readUTF();
        this.description = in.readUTF();
        this.tag_ids = in.readUTF();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.id);
        stmt.setString(2, this.name);
        stmt.setString(3, this.creator_id);
        stmt.setString(4, this.creator_name);
        stmt.setString(5, this.state);
        stmt.setString(6, this.flag);
        stmt.setString(7, this.checker1_id);
        stmt.setString(8, this.checker1_name);
        stmt.setString(9, this.duration);
        stmt.setString(10, this.create_time);
        stmt.setString(11, this.img_url);
        stmt.setString(12, this.media_id);
        stmt.setString(13, this.media_name);
        stmt.setString(14, this.type);
        stmt.setString(15, this.introduction);
        stmt.setString(16, this.category_id);
        stmt.setString(17, this.category_name);
        stmt.setString(18, this.tx_url);
        stmt.setString(19, this.content);
        stmt.setString(20, this.content_url);
        stmt.setString(21, this.remark);
        stmt.setString(22, this.source);
        stmt.setString(23, this.source_id);
        stmt.setString(24, this.size);
        stmt.setString(25, this.checker2_id);
        stmt.setString(26, this.checker2_name);
        stmt.setString(27, this.vod_md5);
        stmt.setString(28, this.bespeak);
        stmt.setString(29, this.bespeak_time);
        stmt.setString(30, this.update_time);
        stmt.setString(31, this.operators);
        stmt.setString(32, this.share_content);
        stmt.setString(33, this.hotel_id);
        stmt.setString(34, this.hotel_name);
        stmt.setString(35, this.room_id);
        stmt.setString(36, this.room_name);
        stmt.setString(37, this.box_id);
        stmt.setString(38, this.box_name);
        stmt.setString(39, this.sort_num);
        stmt.setString(40, this.is_online);
        stmt.setString(41, this.description);
        stmt.setString(42, this.tag_ids);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.id = result.getString(1);
        this.name = result.getString(2);
        this.creator_id = result.getString(3);
        this.creator_name = result.getString(4);
        this.state = result.getString(5);
        this.flag = result.getString(6);
        this.checker1_id = result.getString(7);
        this.checker1_name = result.getString(8);
        this.duration = result.getString(9);
        this.create_time = result.getString(10);
        this.img_url = result.getString(11);
        this.media_id = result.getString(12);
        this.media_name = result.getString(13);
        this.type = result.getString(14);
        this.introduction = result.getString(15);
        this.category_id = result.getString(16);
        this.category_name = result.getString(17);
        this.tx_url = result.getString(18);
        this.content = result.getString(19);
        this.content_url = result.getString(20);
        this.remark = result.getString(21);
        this.source = result.getString(22);
        this.source_id = result.getString(23);
        this.size = result.getString(24);
        this.checker2_id = result.getString(25);
        this.checker2_name = result.getString(26);
        this.vod_md5 = result.getString(27);
        this.bespeak = result.getString(28);
        this.bespeak_time = result.getString(29);
        this.update_time = result.getString(30);
        this.operators = result.getString(31);
        this.share_content = result.getString(32);
        this.hotel_id = result.getString(33);
        this.hotel_name = result.getString(34);
        this.room_id = result.getString(35);
        this.room_name = result.getString(36);
        this.box_id = result.getString(37);
        this.box_name = result.getString(38);
        this.sort_num = result.getString(39);
        this.is_online = result.getString(40);
        this.description = result.getString(41);
        this.tag_ids = result.getString(42);
    }

    @Override
    public String toString() {

        return this.id +
                Constant.VALUE_SPLIT_CHAR + this.name +
                Constant.VALUE_SPLIT_CHAR + this.creator_id +
                Constant.VALUE_SPLIT_CHAR + this.creator_name +
                Constant.VALUE_SPLIT_CHAR + this.state +
                Constant.VALUE_SPLIT_CHAR + this.flag +
                Constant.VALUE_SPLIT_CHAR + this.checker1_id +
                Constant.VALUE_SPLIT_CHAR + this.checker1_name +
                Constant.VALUE_SPLIT_CHAR + this.duration +
                Constant.VALUE_SPLIT_CHAR + this.create_time +
                Constant.VALUE_SPLIT_CHAR + this.img_url +
                Constant.VALUE_SPLIT_CHAR + this.media_id +
                Constant.VALUE_SPLIT_CHAR + this.media_name +
                Constant.VALUE_SPLIT_CHAR + this.type +
                Constant.VALUE_SPLIT_CHAR + this.introduction +
                Constant.VALUE_SPLIT_CHAR + this.category_id +
                Constant.VALUE_SPLIT_CHAR + this.category_name +
                Constant.VALUE_SPLIT_CHAR + this.tx_url +
                Constant.VALUE_SPLIT_CHAR + this.content +
                Constant.VALUE_SPLIT_CHAR + this.content_url +
                Constant.VALUE_SPLIT_CHAR + this.remark +
                Constant.VALUE_SPLIT_CHAR + this.source +
                Constant.VALUE_SPLIT_CHAR + this.source_id +
                Constant.VALUE_SPLIT_CHAR + this.size +
                Constant.VALUE_SPLIT_CHAR + this.checker2_id +
                Constant.VALUE_SPLIT_CHAR + this.checker2_name +
                Constant.VALUE_SPLIT_CHAR + this.vod_md5 +
                Constant.VALUE_SPLIT_CHAR + this.bespeak +
                Constant.VALUE_SPLIT_CHAR + this.bespeak_time +
                Constant.VALUE_SPLIT_CHAR + this.update_time +
                Constant.VALUE_SPLIT_CHAR + this.operators +
                Constant.VALUE_SPLIT_CHAR + this.share_content +
                Constant.VALUE_SPLIT_CHAR + this.hotel_id +
                Constant.VALUE_SPLIT_CHAR + this.hotel_name +
                Constant.VALUE_SPLIT_CHAR + this.room_id +
                Constant.VALUE_SPLIT_CHAR + this.room_name +
                Constant.VALUE_SPLIT_CHAR + this.box_id +
                Constant.VALUE_SPLIT_CHAR + this.box_name +
                Constant.VALUE_SPLIT_CHAR + this.sort_num +
                Constant.VALUE_SPLIT_CHAR + this.is_online +
                Constant.VALUE_SPLIT_CHAR + this.description +
                Constant.VALUE_SPLIT_CHAR + this.tag_ids;
    }

//    public HbaseResources(String text){
//        this.setName(text);
////        Matcher matcher = CommonVariables.MAPPER_INPUT_FORMAT_REGEX.matcher(text);
////        if (!matcher.find()) {
////            return;
////        }
////        this.setName(cleanNull(matcher.group(1)));
//    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }
        return value.trim();
    }

    private String cleanNull(String value){
        if("\\N".equals(value)) {
            return null;
        }

        return value;
    }

}
