package com.littlehotspot.model;

import com.littlehotspot.util.hbase.HBaseColumn;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <h1>模型 -  用户阅读数排序表</h1>
 */
@Data
@NoArgsConstructor
public class UserReadOrderAttrBean {
    /**
     * userid
     */
    @HBaseColumn(name = "user_id")
    private String userId;
    /**
     * 手机类型
     */
    @HBaseColumn(name = "m_type")
    private String mType;
    /**
     * 手机机型
     */
    @HBaseColumn(name = "m_ machine")
    private String mMachine;
    /**
     * 阅读次数
     */
    @HBaseColumn(name = "read_count")
    private String readCount;
    /**
     * 首次下载时间
     */
    @HBaseColumn(name = "f_down_time")
    private String fDownTime;

    /**
     * 首次投屏时间
     */
    @HBaseColumn(name = "f_proje_time")
    private String fProjeTime;

    /**
     * 首次点播时间
     */
    @HBaseColumn(name = "f_dema_time")
    private String fDemaTime;

    public UserReadOrderAttrBean(String text) {
         String [] cols=text.split("\\|",-1);
         this.userId=cols[0];
         this.mType=cols[1];
         this.mMachine=cols[2];
         this.readCount=cols[3];
         this.fDownTime=cols[4];
         this.fProjeTime=cols[5];
         this.fDemaTime=cols[6];
    }
}
