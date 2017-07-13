package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.util.Constant;

import java.util.regex.Pattern;

/**
 * <h1>公共变量 - 数据格式转换(用户投屏点播)</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @notes Created on 2017年06月01日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class CommonVariables extends Constant.CommonVariables {

    /**
     * 开始投屏
     */
    public static final Pattern MAPPER_INPUT_FORMAT_REGEX_START_PRO = Pattern.compile("^(.*),(.*),(.*),(.*),(start),(projection),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

    /**
     * 结束投屏
     */
    public static final Pattern MAPPER_INPUT_FORMAT_REGEX_END_PRO = Pattern.compile("^(.*),(.*),(.*),(.*),(end),(projection),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

    /**
     * 开始点播
     */
    public static final Pattern MAPPER_INPUT_FORMAT_REGEX_START_DEM = Pattern.compile("^(.*),(.*),(.*),(.*),(start),(vod),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

    /**
     * 结束点播
     */
    public static final Pattern MAPPER_INPUT_FORMAT_REGEX_END_DEM = Pattern.compile("^(.*),(.*),(.*),(.*),(end),(vod),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

    /**
     * input regex
     */
    public static final Pattern MAPPER_INPUT_FORMAT_REGEX = Pattern.compile("^(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

    /**
     * final regex
     */
    public static final Pattern MAPPER_INPUT_FORMAT_REGEX_FINAL = Pattern.compile("^(.*),(.*),(.*),(.*),(.*),(end|start),(.*),(.*),(.*),(.*),(.*),(.*),(.*),(.*)$");

}
