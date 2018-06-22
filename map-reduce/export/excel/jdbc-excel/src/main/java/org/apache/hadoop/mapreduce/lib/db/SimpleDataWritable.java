/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.hive
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 20:49
 */
package org.apache.hadoop.mapreduce.lib.db;

import com.sun.tools.jdi.LinkedHashMap;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

/**
 * <h1>读写器 - 简单数据</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月20日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class SimpleDataWritable implements Writable, DBWritable {

    private static final char FIELD_DELIMITER = 0x0001;

    private Map<String, Object> dataMap = new LinkedHashMap();

    public String toString() {
        StringBuilder toString = new StringBuilder();
        if (dataMap != null) {
            int dataMapSize = dataMap.size();
            if (dataMapSize > 0) {
                int column = 1;
                for (Map.Entry<String, Object> entry : dataMap.entrySet()) {
//                String columnLabel = entry.getKey();
                    Object columnValue = entry.getValue();
                    toString.append(columnValue);
                    if (column < dataMapSize) {
                        toString.append(FIELD_DELIMITER);
                    }
                    column++;
                }
            }
        }
        return toString.toString();
    }

    @Override
    public void readFields(ResultSet result) throws SQLException {
        ResultSetMetaData resultSetMetaData = result.getMetaData();
        int columnCount = resultSetMetaData.getColumnCount();
        for (int column = 1; column <= columnCount; column++) {
            String columnLabel = resultSetMetaData.getColumnLabel(column);
//            String columnClassName = resultSetMetaData.getColumnClassName(column);
//            System.out.println(columnLabel + " : " + columnClassName);
            Object columnValue = result.getObject(column);
            dataMap.put(columnLabel, columnValue);
        }
    }

    @Override
    public void write(PreparedStatement stmt) throws SQLException {
//        stmt.setLong(1, this.id);
//        stmt.setString(2, this.name);
//        stmt.setString(3, this.description);
//        stmt.setInt(4, this.duration);
//        stmt.setString(5, this.surfix);
//        stmt.setInt(6, this.type);
//        stmt.setString(7, this.md5);
//        stmt.setString(8, this.ossAddress);
//        stmt.setString(9, this.ossETag);
//        stmt.setString(10, this.filePath);
//        stmt.setLong(11, this.creatorId);
//        stmt.setString(12, this.creator);
//        stmt.setLong(13, this.checkerId);
//        long createTimeTimestamp = this.createTime.getTime();
//        stmt.setTimestamp(14, new Timestamp(createTimeTimestamp));
//        stmt.setInt(15, this.flag);
//        stmt.setInt(16, this.state);
        System.out.println("public void write(PreparedStatement stmt) throws SQLException");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
//        this.id = in.readLong();
//        this.name = Text.readString(in);
        System.out.println("public void readFields(DataInput dataInput) throws IOException");
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
//        out.writeLong(this.id);
//        Text.writeString(out, this.name);
        System.out.println("public void write(DataOutput dataOutput) throws IOException");
    }
}
