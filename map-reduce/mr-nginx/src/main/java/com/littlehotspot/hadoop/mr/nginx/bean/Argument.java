/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.nginx.bean
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 13:59
 */
package com.littlehotspot.hadoop.mr.nginx.bean;

import lombok.Getter;

/**
 * <h1>枚举 - 参数</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public enum Argument {
    HDFSCluster("hdfsCluster", null),
    MapperInputFormatRegex("inRegex", null),
    InputPath("hdfsIn", null),
    InputPathStart("hdfsInStart", null),
    InputPathEnd("hdfsInEnd", null),
    BoxInputPath("hdfsBoxIn", null),
    MobInputPath("hdfsMobIn", null),
    RqInputPath("hdfsRqIn",null),
    DemaInputPath("hdfsDemaIn", null),
    ReadInputPath("hdfsReadIn", null),
    ProInputPath("hdfsProIn",null),
    OldInputPath("hdfsOldIn",null),
    NewInputPath("hdfsNewIn",null),
    UserInputPath("hdfsUserIn", null),
    ActInputPath("hdfsActIn", null),
    NgxInputPath("hdfsNgxIn", null),
    OutputPath("hdfsOut", null),
    HbaseTable("table", null),
    HbaseRoot("hbaseRoot", null),
    HbaseZookeeper("hbaseZookeeper", null),
    HBaseSharePath("hbaseSharePath", null),

    allCountInputPath("hdfsAllCountIn",null),
    uvCountInputPath("hdfsUvIn",null),
    durationInputPath("hdfsDurationIn", null),


    VersionName("versionname", ""),// 版本名称
    VersionCode("versioncode", ""),// 版本号
    BuildVersion("buildversion", ""),// 手机系统版本
    OSVersion("osversion", ""),// 系统 API 版本
    MachineModel("model", ""),// 机器型号
    AppName("appname", ""),// 应用名称
    DeviceId("deviceid", ""),// 设备 ID
    DeviceType("clientname", ""),// 设备类型
    ChannelId("channelid", ""),// 渠道 ID
    ChannelName("channelName", ""),// 渠道名称
    Network("network", ""),// 网络类型
    Language("language", ""),// 语言

    SadActType("sadActType", ""),// 投屏点播类型
    SadType("sadType", ""),// 用户行为类型

    ResourceType("resourceType",""),// 资源类型

    ColumnFamily("columnFamily",""),// 列族名
    ColumnName("columnName",""),// 列名

    JdbcUrl("jdbcUrl", null),
    MysqlUser("mysqlUser", null),
    MysqlPassWord("mysqlPassWord", null),
    Sql("sql", null),

    Time("time", null),
    Before("before", null),

    StartTime("startTime", null),
    EndTime("endTime", null),
    ;

    @Getter
    private String name;

    @Getter
    private String defaultValue;

    private Argument(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }
}
