package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.HBaseTable;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;

/**
 * <h1> 模型 - [目标] 投屏点播-所有天 </h1>
 * Created by Administrator on 2017-06-19 上午 10:56.
 */
@Data
public class TargetSadBaseBean {

    /**
     * rowKey
     */
//    @HBaseColumn(columnName = "rowKey")
    @HBaseRowKey
    private String rowKey;

    /**
     * 区域id
     */
    @HBaseColumn(columnName = "areaId")
    private String areaId;

    /**
     * 区域名称
     */
    @HBaseColumn(columnName = "areaName")
    private String areaName;

    /**
     * 酒店id
     */
    @HBaseColumn(columnName = "hotelId")
    private String hotelId;

    /**
     * 酒店名称
     */
    @HBaseColumn(columnName = "hotelName")
    private String hotelName;

    /**
     * 包间id
     */
    @HBaseColumn(columnName = "roomId")
    private String roomId;

    /**
     * 包间名称
     */
    @HBaseColumn(columnName = "roomName")
    private String roomName;

    /**
     * 盒子id
     */
    @HBaseColumn(columnName = "boxId")
    private String boxId;

    /**
     * 盒子名称
     */
    @HBaseColumn(columnName = "boxName")
    private String boxName;

    /**
     * 盒子mac
     */
    @HBaseColumn(columnName = "boxMac")
    private String boxMac;

    /**
     * 手机标识
     */
    @HBaseColumn(columnName = "mobileId")
    private String mobileId;

    /**
     * 投屏数量
     */
    @HBaseColumn(columnName = "projectCount")
    private String projectCount;

    /**
     * 点播数量
     */
    @HBaseColumn(columnName = "demandCount")
    private String demandCount;

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
        rowLine.append(this.getRowKey() == null ? "" : this.getRowKey()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getAreaId() == null ? "" : this.getAreaId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getAreaName() == null ? "" : this.getAreaName()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getHotelId() == null ? "" : this.getHotelId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getHotelName() == null ? "" : this.getHotelName()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getRoomId() == null ? "" : this.getRoomId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getRoomName() == null ? "" : this.getRoomName()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getBoxId() == null ? "" : this.getBoxId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getBoxName() == null ? "" : this.getBoxName()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getBoxMac() == null ? "" : this.getBoxMac()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getMobileId() == null ? "" : this.getMobileId()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getProjectCount() == null ? "" : this.getProjectCount()).append(Constant.VALUE_SPLIT_CHAR);
        rowLine.append(this.getDemandCount() == null ? "" : this.getDemandCount()).append(Constant.VALUE_SPLIT_CHAR);
        return rowLine.toString();
    }


}
