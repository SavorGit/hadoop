package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import lombok.Data;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-06-26 下午 4:55.
 */
@Data
public class TextTargetSadAttrBean {

    @HBaseRowKey
    private String rowKey;

    @HBaseColumn(columnName = "device_id")
    private String device_id;

    @HBaseColumn(columnName = "start")
    private long start;

    @HBaseColumn(columnName = "end")
    private long end;

    @HBaseColumn(columnName = "type")
    private String type;

    @HBaseColumn(columnName = "p_time")
    private long p_time;

    public void setP_time() {
        this.p_time = this.end-this.start;
    }

    public void setStart(long start) {
        this.start = start;
        this.setP_time();
    }

    public void setEnd(long end) {
        this.end = end;
        this.setP_time();
    }

    //    public String rowLine() {
//        StringBuffer rowLine = new StringBuffer();
//        rowLine.append(this.getRowKey() == null ? "" : this.getRowKey()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getUuid() == null ? "" : this.getUuid()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getHotel_id() == null ? "" : this.getHotel_id()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getRoom_id() == null ? "" : this.getRoom_id()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getStart_timestamps() == null ? "" : this.getStart_timestamps()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getEnd_timestamps() == null ? "" : this.getEnd_timestamps()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getDuration() == null ? "" : this.getDuration()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getOption_type() == null ? "" : this.getOption_type()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getMedia_type() == null ? "" : this.getMedia_type()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getMedia_id() == null ? "" : this.getMedia_id()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getMobile_id() == null ? "" : this.getMobile_id()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getApk_version() == null ? "" : this.getApk_version()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getAds_period() == null ? "" : this.getAds_period()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getDemand_period() == null ? "" : this.getDemand_period()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getCommon_value() == null ? "" : this.getCommon_value()).append(Constant.VALUE_SPLIT_CHAR);
//        rowLine.append(this.getMac() == null ? "" : this.getMac()).append(Constant.VALUE_SPLIT_CHAR);
//        return rowLine.toString();
//    }
}
