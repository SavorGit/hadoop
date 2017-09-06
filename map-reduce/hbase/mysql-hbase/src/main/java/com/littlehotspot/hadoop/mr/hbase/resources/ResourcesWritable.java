package com.littlehotspot.hadoop.mr.hbase.resources;

import com.littlehotspot.hadoop.mr.hbase.AbstractWritable;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-28 下午 5:05.
 */
public class ResourcesWritable extends AbstractWritable implements Writable, DBWritable {
    @Setter
    @Getter
    private Long rety;
    @Setter
    @Getter
    private Long id;

    private String name;
    private Long crt_id;
    private String crt_name;
    private Integer state;
    private Integer flag;
    private Integer chk1_id;
    private String chk1_name;
    private Integer duration;
    private Long create_time;
    private String img_url;
    private Long mda_id;
    private String mda_name;
    private Integer type;

    private String introduction;
    private Integer caty_id;
    private String caty_name;
    private String tx_url;
    private String content;
    private String content_url;
    private String remark;
    private String source;
    private Integer source_id;
    private String size;
    private Integer chk2_id;
    private String chk2_name;
    private String vod_md5;
    private String bespk;
    private Long bespk_time;
    private Long update_time;
    private String operators;
    private String share_cont;
    private Long hotel_id;
    private String hotel_name;
    private Long room_id;
    private String room_name;
    private Long box_id;
    private String box_name;
    private Integer sort_num;
    private Integer is_online;
    private String description;
    private String tag_ids;

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setLong(1, this.rety);
        statement.setLong(2, this.id);
        statement.setString(3, this.name);
        statement.setLong(4, this.crt_id);
        statement.setString(5, this.crt_name);
        statement.setInt(6, this.state);
        statement.setInt(7, this.flag);
        statement.setInt(8, this.chk1_id);
        statement.setString(9, this.chk1_name);
        statement.setInt(10, this.duration);
        statement.setLong(11, this.create_time);
        statement.setString(12, this.img_url);
        statement.setLong(13, this.mda_id);
        statement.setString(14, this.mda_name);
        statement.setInt(15, this.type);
        statement.setString(16, this.introduction);
        statement.setInt(17, this.caty_id);
        statement.setString(18, this.caty_name);
        statement.setString(19, this.tx_url);
        statement.setString(20, this.content);
        statement.setString(21, this.content_url);
        statement.setString(22, this.remark);
        statement.setString(23, this.source);
        statement.setInt(24, this.source_id);
        statement.setString(25, this.size);
        statement.setInt(26, this.chk2_id);
        statement.setString(27, this.chk2_name);
        statement.setString(28, this.vod_md5);
        statement.setString(29, this.bespk);
        statement.setLong(30, this.bespk_time);
        statement.setLong(31, this.update_time);
        statement.setString(32, this.operators);
        statement.setString(33, this.share_cont);
        statement.setLong(34, this.hotel_id);
        statement.setString(35, this.hotel_name);
        statement.setLong(36, this.room_id);
        statement.setString(37, this.room_name);
        statement.setLong(38, this.box_id);
        statement.setString(39, this.box_name);
        statement.setLong(40, this.sort_num);
        statement.setLong(41, this.is_online);
        statement.setString(42, this.description);
        statement.setString(43, this.tag_ids);
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        this.setValue(ResourcesWritable.class, this, "rety", result, "rety");
        this.setValue(ResourcesWritable.class, this, "id", result, "id");
        this.setValue(ResourcesWritable.class, this, "name", result, "name");
        this.setValue(ResourcesWritable.class, this, "crt_id", result, "crt_id");
        this.setValue(ResourcesWritable.class, this, "crt_name", result, "crt_name");
        this.setValue(ResourcesWritable.class, this, "state", result, "state");
        this.setValue(ResourcesWritable.class, this, "flag", result, "flag");
        this.setValue(ResourcesWritable.class, this, "chk1_id", result, "chk1_id");
        this.setValue(ResourcesWritable.class, this, "chk1_name", result, "chk1_name");
        this.setValue(ResourcesWritable.class, this, "duration", result, "duration");
        this.setValue(ResourcesWritable.class, this, "create_time", result, "create_time");
        this.setValue(ResourcesWritable.class, this, "img_url", result, "img_url");
        this.setValue(ResourcesWritable.class, this, "mda_id", result, "mda_id");
        this.setValue(ResourcesWritable.class, this, "mda_name", result, "mda_name");
        this.setValue(ResourcesWritable.class, this, "type", result, "type");
        this.setValue(ResourcesWritable.class, this, "introduction", result, "introduction");
        this.setValue(ResourcesWritable.class, this, "caty_id", result, "caty_id");
        this.setValue(ResourcesWritable.class, this, "caty_name", result, "caty_name");
        this.setValue(ResourcesWritable.class, this, "caty_name", result, "caty_name");
        this.setValue(ResourcesWritable.class, this, "content", result, "content");
        this.setValue(ResourcesWritable.class, this, "content_url", result, "content_url");
        this.setValue(ResourcesWritable.class, this, "remark", result, "remark");
        this.setValue(ResourcesWritable.class, this, "source", result, "source");
        this.setValue(ResourcesWritable.class, this, "source_id", result, "source_id");
        this.setValue(ResourcesWritable.class, this, "size", result, "size");
        this.setValue(ResourcesWritable.class, this, "chk2_id", result, "chk2_id");
        this.setValue(ResourcesWritable.class, this, "chk2_name", result, "chk2_name");
        this.setValue(ResourcesWritable.class, this, "vod_md5", result, "vod_md5");
        this.setValue(ResourcesWritable.class, this, "bespk", result, "bespk");
        this.setValue(ResourcesWritable.class, this, "bespk_time", result, "bespk_time");
        this.setValue(ResourcesWritable.class, this, "update_time", result, "update_time");
        this.setValue(ResourcesWritable.class, this, "operators", result, "operators");
        this.setValue(ResourcesWritable.class, this, "share_cont", result, "share_cont");
        this.setValue(ResourcesWritable.class, this, "hotel_id", result, "hotel_id");
        this.setValue(ResourcesWritable.class, this, "hotel_name", result, "hotel_name");
        this.setValue(ResourcesWritable.class, this, "room_id", result, "room_id");
        this.setValue(ResourcesWritable.class, this, "room_name", result, "room_name");
        this.setValue(ResourcesWritable.class, this, "box_id", result, "box_id");
        this.setValue(ResourcesWritable.class, this, "box_name", result, "box_name");
        this.setValue(ResourcesWritable.class, this, "sort_num", result, "sort_num");
        this.setValue(ResourcesWritable.class, this, "is_online", result, "is_online");
        this.setValue(ResourcesWritable.class, this, "description", result, "description");
        this.setValue(ResourcesWritable.class, this, "tag_ids", result, "tag_ids");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.id);
        Text.writeString(dataOutput, this.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readLong();
        String line = Text.readString(dataInput);
        this.buildBean(line);
    }

    @Override
    public String toString() {
        StringBuilder toString;
        toString = new StringBuilder();
        toString.append(this.rety).append(FIELD_DELIMITER);
        toString.append(this.id).append(FIELD_DELIMITER);
        toString.append(this.name).append(FIELD_DELIMITER);
        toString.append(this.crt_id).append(FIELD_DELIMITER);
        toString.append(this.crt_name).append(FIELD_DELIMITER);
        toString.append(this.state).append(FIELD_DELIMITER);
        toString.append(this.flag).append(FIELD_DELIMITER);
        toString.append(this.chk1_id).append(FIELD_DELIMITER);
        toString.append(this.chk1_name).append(FIELD_DELIMITER);
        toString.append(this.duration).append(FIELD_DELIMITER);
        toString.append(this.create_time).append(FIELD_DELIMITER);
        toString.append(this.img_url).append(FIELD_DELIMITER);
        toString.append(this.mda_id).append(FIELD_DELIMITER);
        toString.append(this.mda_name).append(FIELD_DELIMITER);
        toString.append(this.type).append(FIELD_DELIMITER);
        toString.append(this.introduction).append(FIELD_DELIMITER);
        toString.append(this.caty_id).append(FIELD_DELIMITER);
        toString.append(this.caty_name).append(FIELD_DELIMITER);
        toString.append(this.tx_url).append(FIELD_DELIMITER);
        toString.append(this.content).append(FIELD_DELIMITER);
        toString.append(this.content_url).append(FIELD_DELIMITER);
        toString.append(this.remark).append(FIELD_DELIMITER);
        toString.append(this.source).append(FIELD_DELIMITER);
        toString.append(this.source_id).append(FIELD_DELIMITER);
        toString.append(this.size).append(FIELD_DELIMITER);
        toString.append(this.chk2_id).append(FIELD_DELIMITER);
        toString.append(this.chk2_name).append(FIELD_DELIMITER);
        toString.append(this.vod_md5).append(FIELD_DELIMITER);
        toString.append(this.bespk).append(FIELD_DELIMITER);
        toString.append(this.bespk_time).append(FIELD_DELIMITER);
        toString.append(this.update_time).append(FIELD_DELIMITER);
        toString.append(this.operators).append(FIELD_DELIMITER);
        toString.append(this.share_cont).append(FIELD_DELIMITER);
        toString.append(this.hotel_id).append(FIELD_DELIMITER);
        toString.append(this.hotel_name).append(FIELD_DELIMITER);
        toString.append(this.room_id).append(FIELD_DELIMITER);
        toString.append(this.room_name).append(FIELD_DELIMITER);
        toString.append(this.box_id).append(FIELD_DELIMITER);
        toString.append(this.box_name).append(FIELD_DELIMITER);
        toString.append(this.sort_num).append(FIELD_DELIMITER);
        toString.append(this.is_online).append(FIELD_DELIMITER);
        toString.append(this.description).append(FIELD_DELIMITER);
        toString.append(this.tag_ids);

        return super.toString();
    }

    public Put toPut() {
        if (this.id == null) {
            throw new IllegalStateException("The id of resources-bean for function[toPut]");
        }
        long version = System.currentTimeMillis();
        Put put = new Put(Bytes.toBytes(this.id + "|" + this.rety));// 设置rowkey

        // 基本属性
        String familyName = "attr";
        this.addColumn(ResourcesWritable.class, this, "rety", put, familyName, "rety", version);
        this.addColumn(ResourcesWritable.class, this, "id", put, familyName, "id", version);
        this.addColumn(ResourcesWritable.class, this, "name", put, familyName, "name", version);
        this.addColumn(ResourcesWritable.class, this, "crt_id", put, familyName, "crt_id", version);
        this.addColumn(ResourcesWritable.class, this, "crt_name", put, familyName, "crt_name", version);
        this.addColumn(ResourcesWritable.class, this, "state", put, familyName, "state", version);
        this.addColumn(ResourcesWritable.class, this, "flag", put, familyName, "flag", version);
        this.addColumn(ResourcesWritable.class, this, "chk1_id", put, familyName, "chk1_id", version);
        this.addColumn(ResourcesWritable.class, this, "chk1_name", put, familyName, "chk1_name", version);
        this.addColumn(ResourcesWritable.class, this, "duration", put, familyName, "duration", version);
        this.addColumn(ResourcesWritable.class, this, "create_time", put, familyName, "create_time", version);
        this.addColumn(ResourcesWritable.class, this, "img_url", put, familyName, "img_url", version);
        this.addColumn(ResourcesWritable.class, this, "mda_id", put, familyName, "mda_id", version);
        this.addColumn(ResourcesWritable.class, this, "mda_name", put, familyName, "mda_name", version);
        this.addColumn(ResourcesWritable.class, this, "type", put, familyName, "type", version);

        // 附加属性
        String familyName1 = "adat";
        this.addColumn(ResourcesWritable.class, this, "introduction", put, familyName1, "introduction", version);
        this.addColumn(ResourcesWritable.class, this, "caty_id", put, familyName1, "caty_id", version);
        this.addColumn(ResourcesWritable.class, this, "caty_name", put, familyName1, "caty_name", version);
        this.addColumn(ResourcesWritable.class, this, "tx_url", put, familyName1, "tx_url", version);
        this.addColumn(ResourcesWritable.class, this, "content", put, familyName1, "content", version);
        this.addColumn(ResourcesWritable.class, this, "content_url", put, familyName1, "content_url", version);
        this.addColumn(ResourcesWritable.class, this, "remark", put, familyName1, "remark", version);
        this.addColumn(ResourcesWritable.class, this, "source", put, familyName1, "source", version);
        this.addColumn(ResourcesWritable.class, this, "source_id", put, familyName1, "source_id", version);
        this.addColumn(ResourcesWritable.class, this, "size", put, familyName1, "size", version);
        this.addColumn(ResourcesWritable.class, this, "chk2_id", put, familyName1, "chk2_id", version);
        this.addColumn(ResourcesWritable.class, this, "chk2_name", put, familyName1, "chk2_name", version);
        this.addColumn(ResourcesWritable.class, this, "vod_md5", put, familyName1, "vod_md5", version);
        this.addColumn(ResourcesWritable.class, this, "bespk", put, familyName1, "bespk", version);
        this.addColumn(ResourcesWritable.class, this, "bespk_time", put, familyName1, "bespk_time", version);
        this.addColumn(ResourcesWritable.class, this, "update_time", put, familyName1, "update_time", version);
        this.addColumn(ResourcesWritable.class, this, "operators", put, familyName1, "operators", version);
        this.addColumn(ResourcesWritable.class, this, "share_cont", put, familyName1, "share_cont", version);
        this.addColumn(ResourcesWritable.class, this, "hotel_id", put, familyName1, "hotel_id", version);
        this.addColumn(ResourcesWritable.class, this, "hotel_name", put, familyName1, "hotel_name", version);
        this.addColumn(ResourcesWritable.class, this, "room_id", put, familyName1, "room_id", version);
        this.addColumn(ResourcesWritable.class, this, "room_name", put, familyName1, "room_name", version);
        this.addColumn(ResourcesWritable.class, this, "box_id", put, familyName1, "box_id", version);
        this.addColumn(ResourcesWritable.class, this, "box_name", put, familyName1, "box_name", version);
        this.addColumn(ResourcesWritable.class, this, "sort_num", put, familyName1, "sort_num", version);
        this.addColumn(ResourcesWritable.class, this, "is_online", put, familyName1, "is_online", version);
        this.addColumn(ResourcesWritable.class, this, "description", put, familyName1, "description", version);
        this.addColumn(ResourcesWritable.class, this, "tag_ids", put, familyName1, "tag_ids", version);

        return put;
    }

    private void buildBean(String line) {

    }
}
