/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : org.apache.hadoop.mapreduce.lib.db
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 18:39
 */
package org.apache.hadoop.mapreduce.lib.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * A InputFormat that reads input data from an Hive table.
 * <p>
 * HiveInputFormat emits LongWritables containing the record number as
 * key and HiveWritables as value.
 * <p>
 * The Hive query, and input class can be using one of the two
 * setInput methods.
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月19日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class HiveInputFormat<T extends DBWritable> extends DBInputFormat<T> {

    protected RecordReader<LongWritable, T> createDBRecordReader(InputSplit split, Configuration conf) throws IOException {
        @SuppressWarnings("unchecked")
        Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
        try {
            // use database product name to determine appropriate record reader.
            if (dbProductName.startsWith("ORACLE")) {
                // use Oracle-specific db reader.
                return new OracleDBRecordReader<>((HiveInputFormat.DBInputSplit) split, inputClass, conf, getConnection(), getDBConf(), conditions, fieldNames, tableName);
            } else if (dbProductName.startsWith("MYSQL")) {
                // use MySQL-specific db reader.
                return new MySQLDBRecordReader<>((HiveInputFormat.DBInputSplit) split, inputClass, conf, getConnection(), getDBConf(), conditions, fieldNames, tableName);
            } else if (dbProductName.startsWith("APACHE HIVE")) {
                // use MySQL-specific db reader.
                return new HiveDBRecordReader<>((HiveInputFormat.DBInputSplit) split, inputClass, conf, getConnection(), getDBConf(), conditions, fieldNames, tableName);
            } else {
                // Generic reader.
                return new DBRecordReader<>((HiveInputFormat.DBInputSplit) split, inputClass, conf, getConnection(), getDBConf(), conditions, fieldNames, tableName);
            }
        } catch (SQLException ex) {
            throw new IOException(ex.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RecordReader<LongWritable, T> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return createDBRecordReader(split, context.getConfiguration());
    }

    /**
     * {@inheritDoc}
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        ResultSet results = null;
        Statement statement = null;
        try {
            statement = connection.createStatement();

            results = statement.executeQuery(getCountQuery());
            results.next();

            long count = results.getLong(1);
            int chunks = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);
            long chunkSize = (count / chunks);

            results.close();
            statement.close();

            List<InputSplit> splits = new ArrayList<>();

            // Split the rows into n-number of chunks and adjust the last chunk
            // accordingly
            for (int i = 0; i < chunks; i++) {
                HiveInputFormat.DBInputSplit split;
                if ((i + 1) == chunks) {
                    split = new HiveInputFormat.DBInputSplit(i * chunkSize, count);
                } else {
                    split = new HiveInputFormat.DBInputSplit(i * chunkSize, (i * chunkSize) + chunkSize);
                }
                splits.add(split);
            }
            return splits;
        } catch (SQLException e) {
            throw new IOException("Got SQLException", e);
        } finally {
            try {
                if (results != null) {
                    results.close();
                }
            } catch (SQLException e1) {
                //
            }
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException e1) {
                //
            }

            closeConnection();
        }
    }

    /**
     * Initializes the map-part of the job with the appropriate input settings.
     *
     * @param job        The map-reduce job
     * @param inputClass the class object implementing DBWritable, which is the
     *                   Java object holding tuple fields.
     * @param tableName  The table to read data from
     * @param conditions The condition which to select data with,
     *                   eg. '(updated > 20070101 AND length > 0)'
     * @param orderBy    the fieldNames in the orderBy clause.
     * @param fieldNames The field names in the table
     * @see #setInput(Job, Class, String, String)
     */
    public static void setInput(Job job, Class<? extends DBWritable> inputClass, String tableName, String conditions, String orderBy, String... fieldNames) {
        job.setInputFormatClass(HiveInputFormat.class);
        DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
        dbConf.setInputClass(inputClass);
        dbConf.setInputTableName(tableName);
        dbConf.setInputFieldNames(fieldNames);
        dbConf.setInputConditions(conditions);
        dbConf.setInputOrderBy(orderBy);
    }

    /**
     * Initializes the map-part of the job with the appropriate input settings.
     *
     * @param job             The map-reduce job
     * @param inputClass      the class object implementing DBWritable, which is the
     *                        Java object holding tuple fields.
     * @param inputQuery      the input query to select fields. Example :
     *                        "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
     * @param inputCountQuery the input query that returns
     *                        the number of records in the table.
     *                        Example : "SELECT COUNT(f1) FROM Mytable"
     * @see #setInput(Job, Class, String, String, String, String...)
     */
    public static void setInput(Job job, Class<? extends DBWritable> inputClass, String inputQuery, String inputCountQuery) {
        job.setInputFormatClass(HiveInputFormat.class);
        DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
        dbConf.setInputClass(inputClass);
        dbConf.setInputQuery(inputQuery);
        dbConf.setInputCountQuery(inputCountQuery);
    }
}
