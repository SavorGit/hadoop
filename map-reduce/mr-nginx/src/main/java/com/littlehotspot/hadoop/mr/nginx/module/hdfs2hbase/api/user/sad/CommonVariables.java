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
     * Mapper 输入时正则过滤
     */
    public static Pattern MAPPER_INPUT_FORMAT_REGEX_START_PRO = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(start)\u0001(projection)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static Pattern MAPPER_INPUT_FORMAT_REGEX_END_PRO = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(end)\u0001(projection)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static Pattern MAPPER_INPUT_FORMAT_REGEX_START_DEM = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(start)\u0001(vod)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static Pattern MAPPER_INPUT_FORMAT_REGEX_END_DEM = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(end)\u0001(vod)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static Pattern MAPPER_INPUT_FORMAT_REGEX = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static Pattern MAPPER_INPUT_FORMAT_REGEX_FINAL = Pattern.compile("^(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(end|start)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)\u0001(.*)$");

    public static HBaseHelper hBaseHelper;
}
