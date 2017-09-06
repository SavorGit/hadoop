package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.resources;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-07-07 下午 3:15.
 */
public enum ResourceType {
    /**
     * 类型
     * 0x0001：内容，
     * 0x0101：广告，
     * 0x0102：宣传片，
     * 0x0103：节目
     */
    CON(0x0001), // 1
    ADS(0x0101), // 257
    ADV(0x0102), // 258
    PRO(0x0103) // 259

    ;
    private int value;

    public int getValue() {
        return value;
    }

    ResourceType(int value) {
        this.value = value;
    }
}
