/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : org.apache.hadoop.mapreduce.lib.db
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 20:33
 */
package org.apache.hadoop.mapreduce.lib.db;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;

/**
 * A RecordReader that reads records from a Hive table.
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月20日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HiveDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

    private ResultSet results = null;

    /**
     * @param split      The InputSplit to read data for
     * @param inputClass
     * @param conf
     * @param conn
     * @param dbConfig
     * @param cond
     * @param fields
     * @param table      @throws SQLException
     */
    public HiveDBRecordReader(HiveInputFormat.DBInputSplit split, Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig, String cond, String[] fields, String table) throws SQLException {
        super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
    }

    /**
     * Returns the query for selecting the records,
     * subclasses can override this for custom behaviour.
     */
    @Override
    protected String getSelectQuery() {
        StringBuilder query = new StringBuilder();
        StringBuilder realQuery = new StringBuilder();
        StringBuilder fieldNames = new StringBuilder();

        int chunks = this.getDBConf().getConf().getInt(MRJobConfig.NUM_MAPS, 1);

        // Default codepath for MySQL, HSQLDB, etc. Relies on LIMIT/OFFSET for splits.
        if (this.getDBConf().getInputQuery() == null) {
            query.append("SELECT ");

            for (int i = 0; i < this.getFieldNames().length; i++) {
                query.append(this.getFieldNames()[i]);
                fieldNames.append(this.getFieldNames()[i]);
                if (i != this.getFieldNames().length - 1) {
                    query.append(", ");
                    fieldNames.append(", ");
                }
            }

            query.append(" FROM ").append(this.getTableName());
            query.append(" AS ").append(this.getTableName()); //in hsqldb this is necessary
            if (this.getConditions() != null && this.getConditions().length() > 0) {
                query.append(" WHERE (").append(this.getConditions()).append(")");
            }

            String orderBy = this.getDBConf().getInputOrderBy();
            if (orderBy != null && orderBy.length() > 0) {
                query.append(" ORDER BY ").append(orderBy);
            }
        } else {
            //PREBUILT QUERY
            String inputQuery = this.getDBConf().getInputQuery();
            String inputQueryToUpperCase = inputQuery.toUpperCase();
            int firstSelectIndex = inputQueryToUpperCase.indexOf("SELECT ");
            if (firstSelectIndex < 0) {
                throw new RuntimeException(new SQLSyntaxErrorException("Not found select clause in SQL '" + inputQuery + "'"));
            }
            int firstFromIndex = inputQueryToUpperCase.indexOf(" FROM ");
            if (firstFromIndex < 0) {
                throw new RuntimeException(new SQLSyntaxErrorException("Not found from clause in SQL '" + inputQuery + "'"));
            }
            String fieldNamesString = inputQuery.substring(firstSelectIndex + "SELECT ".length(), firstFromIndex);
            fieldNames.append(fieldNamesString);
            query.append(inputQuery);
        }

        if (chunks < 1) {
            return null;
        } else if (chunks == 1) {
            realQuery.append(query);
        } else {
            realQuery.append("SELECT ").append(fieldNames).append(" FROM (SELECT row_number() OVER () AS sys_row_num_, sys_table_1_.* FROM (").append(query).append(") AS sys_table_1_) AS sys_table_2_ WHERE sys_table_2_.sys_row_num_ BETWEEN ").append(this.getSplit().getStart() + 1).append(" AND ").append(this.getSplit().getEnd());
        }

        System.out.println("HiveQL : " + realQuery);
        return realQuery.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        try {
            if (null != this.results) {
                results.close();
            }
            if (null != statement) {
                statement.close();
            }
            if (null != this.getConnection()) {
                this.getConnection().close();
            }
        } catch (SQLException e) {
            throw new IOException(e.getMessage());
        }
    }
}
