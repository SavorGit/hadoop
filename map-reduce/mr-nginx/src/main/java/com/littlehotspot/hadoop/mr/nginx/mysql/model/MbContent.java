package com.littlehotspot.hadoop.mr.nginx.mysql.model;

import lombok.Data;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <h1> 内容 </h1>
 * Created by Administrator on 2017-07-04 下午 6:00.
 */
@Data
public class MbContent extends Model {

    private String id;

    private String title;

    private String creator_id;



    private String share_title;

    private String img_url;

    private String index_img_url;

    private String duration;

    private String category_id;

    private String hot_category_id;

    private String sort_num;

    private String tx_url;

    private String content;

    private String content_url;

    private String remark;

    private String source;

    private String source_id;

    private String size;

    private String state;

    private String cheaker1_id;

    private String media_id;

    private String vod_md5;

    private String bespeak;

    private String bespeak_time;

    private String cheaker2_id;

    private String create_time;

    private String update_time;

    private String operators;

    private String type;

    private String share_content;

    private String content_word_num;

    private String sort_tag;

    private String order_tag;

    @Override
    public String toString() {
        return id + ','
                + title + ','
                + share_title + ','
                + img_url + ','
                + index_img_url + ','
                + duration + ','
                + category_id + ','
                + hot_category_id + ','
                + sort_num + ','
                + tx_url + ','
                + content + ','
                + content_url + ','
                + remark + ','
                + source + ','
                + source_id + ','
                + size + ','
                + state + ','
                + creator_id + ','
                + cheaker1_id + ','
                + media_id + ','
                + vod_md5 + ','
                + bespeak + ','
                + bespeak_time + ','
                + cheaker2_id + ','
                + create_time + ','
                + update_time + ','
                + operators + ','
                + type + ','
                + share_content + ','
                + content_word_num + ','
                + sort_tag + ','
                + order_tag;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out,this.id);
        Text.writeString(out,this.title);
        Text.writeString(out,this.share_title);
        Text.writeString(out,this.img_url);
        Text.writeString(out,this.index_img_url);
        Text.writeString(out,this.duration);
        Text.writeString(out,this.category_id);
        Text.writeString(out,this.hot_category_id);
        Text.writeString(out,this.sort_num);
        Text.writeString(out,this.tx_url);
        Text.writeString(out,this.content);
        Text.writeString(out,this.content_url);
        Text.writeString(out,this.remark);
        Text.writeString(out,this.source);
        Text.writeString(out,this.source_id);
        Text.writeString(out,this.size);
        Text.writeString(out,this.state);
        Text.writeString(out,this.creator_id);
        Text.writeString(out,this.cheaker1_id);
        Text.writeString(out,this.media_id);
        Text.writeString(out,this.vod_md5);
        Text.writeString(out,this.bespeak);
        Text.writeString(out,this.bespeak_time);
        Text.writeString(out,this.cheaker2_id);
        Text.writeString(out,this.create_time);
        Text.writeString(out,this.update_time);
        Text.writeString(out,this.operators);
        Text.writeString(out,this.type);
        Text.writeString(out,this.share_content);
        Text.writeString(out,this.content_word_num);
        Text.writeString(out,this.sort_tag);
        Text.writeString(out,this.order_tag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.title = in.readUTF();
        this.share_title = in.readUTF();
        this.img_url = in.readUTF();
        this.index_img_url = in.readUTF();
        this.duration = in.readUTF();
        this.category_id = in.readUTF();
        this.hot_category_id = in.readUTF();
        this.sort_num = in.readUTF();
        this.tx_url = in.readUTF();
        this.content = in.readUTF();
        this.content_url = in.readUTF();
        this.remark = in.readUTF();
        this.source = in.readUTF();
        this.source_id = in.readUTF();
        this.size = in.readUTF();
        this.state = in.readUTF();
        this.creator_id = in.readUTF();
        this.cheaker1_id = in.readUTF();
        this.media_id = in.readUTF();
        this.vod_md5 = in.readUTF();
        this.bespeak = in.readUTF();
        this.bespeak_time = in.readUTF();
        this.cheaker2_id = in.readUTF();
        this.create_time = in.readUTF();
        this.update_time = in.readUTF();
        this.operators = in.readUTF();
        this.type = in.readUTF();
        this.share_content = in.readUTF();
        this.content_word_num = in.readUTF();
        this.sort_tag = in.readUTF();
        this.order_tag = in.readUTF();
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
        stmt.setString(1, this.id);
        stmt.setString(2, this.title);
        stmt.setString(3, this.share_title);
        stmt.setString(4, this.img_url);
        stmt.setString(5, this.index_img_url);
        stmt.setString(6, this.duration);
        stmt.setString(7, this.category_id);
        stmt.setString(8, this.hot_category_id);
        stmt.setString(9, this.sort_num);
        stmt.setString(10, this.tx_url);
        stmt.setString(11, this.content);
        stmt.setString(12, this.content_url);
        stmt.setString(13, this.remark);
        stmt.setString(14, this.source);
        stmt.setString(15, this.source_id);
        stmt.setString(16, this.size);
        stmt.setString(17, this.state);
        stmt.setString(18, this.creator_id);
        stmt.setString(19, this.cheaker1_id);
        stmt.setString(20, this.media_id);
        stmt.setString(21, this.vod_md5);
        stmt.setString(22, this.bespeak);
        stmt.setString(23, this.bespeak_time);
        stmt.setString(24, this.cheaker2_id);
        stmt.setString(25, this.create_time);
        stmt.setString(26, this.update_time);
        stmt.setString(27, this.operators);
        stmt.setString(28, this.type);
        stmt.setString(29, this.share_content);
        stmt.setString(30, this.content_word_num);
        stmt.setString(31, this.sort_tag);
        stmt.setString(32, this.order_tag);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.id = result.getString(1);
        this.title = result.getString(2);
        this.share_title = result.getString(3);
        this.img_url = result.getString(4);
        this.index_img_url = result.getString(5);
        this.duration = result.getString(6);
        this.category_id = result.getString(7);
        this.hot_category_id = result.getString(8);
        this.sort_num = result.getString(9);
        this.tx_url = result.getString(10);
        this.content = result.getString(11);
        this.content_url = result.getString(12);
        this.remark = result.getString(13);
        this.source = result.getString(14);
        this.source_id = result.getString(15);
        this.size = result.getString(16);
        this.state = result.getString(17);
        this.creator_id = result.getString(18);
        this.cheaker1_id = result.getString(19);
        this.media_id = result.getString(20);
        this.vod_md5 = result.getString(21);
        this.bespeak = result.getString(22);
        this.bespeak_time = result.getString(23);
        this.cheaker2_id = result.getString(24);
        this.create_time = result.getString(25);
        this.update_time = result.getString(26);
        this.operators = result.getString(27);
        this.type = result.getString(28);
        this.share_content = result.getString(29);
        this.content_word_num = result.getString(30);
        this.sort_tag = result.getString(31);
        this.order_tag = result.getString(32);
    }
}
