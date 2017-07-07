package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseColumn;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseRowKey;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;
import lombok.Data;

/**
 * <h1> 模型 - [目标] 投屏点播-所有天 </h1>
 * Created by Administrator on 2017-06-19 上午 10:56.
 */
@Data
public class TargetSadBaseBean {

    /**
     * 区域id
     */
    @HBaseColumn(name = "areaId")
    private String areaId;

    /**
     * 区域名称
     */
    @HBaseColumn(name = "areaName")
    private String areaName;

    /**
     * 酒店id
     */
    @HBaseColumn(name = "hotelId")
    private String hotelId;

    /**
     * 酒店名称
     */
    @HBaseColumn(name = "hotelName")
    private String hotelName;

    /**
     * 包间id
     */
    @HBaseColumn(name = "roomId")
    private String roomId;

    /**
     * 包间名称
     */
    @HBaseColumn(name = "roomName")
    private String roomName;

    /**
     * 盒子id
     */
    @HBaseColumn(name = "boxId")
    private String boxId;

    /**
     * 盒子名称
     */
    @HBaseColumn(name = "boxName")
    private String boxName;

    /**
     * 盒子mac
     */
    @HBaseColumn(name = "boxMac")
    private String boxMac;

    /**
     * 手机标识
     */
    @HBaseColumn(name = "mobileId")
    private String mobileId;

    /**
     * 投屏数量
     */
    @HBaseColumn(name = "projectCount")
    private String projectCount;

    /**
     * 点播数量
     */
    @HBaseColumn(name = "demandCount")
    private String demandCount;

    public String rowLine() {
        StringBuffer rowLine = new StringBuffer();
//        rowLine.append(this.getRowKey() == null ? "" : this.getRowKey()).append(Constant.VALUE_SPLIT_CHAR);
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
