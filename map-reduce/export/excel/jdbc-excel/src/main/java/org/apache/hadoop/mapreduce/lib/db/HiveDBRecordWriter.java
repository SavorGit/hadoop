/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : org.apache.hadoop.mapreduce.lib.db
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:50
 */
package org.apache.hadoop.mapreduce.lib.db;

import lombok.Getter;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * A RecordWriter that writes the reduce output to a Hive table
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月22日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
@InterfaceStability.Evolving
public class HiveDBRecordWriter<K extends DBWritable, V> extends RecordWriter<K, V> {

    @Getter
    private Connection connection;

    @Getter
    private PreparedStatement statement;

    public HiveDBRecordWriter() throws SQLException {
    }

    public HiveDBRecordWriter(Connection connection, PreparedStatement statement) throws SQLException {
        this.connection = connection;
        this.statement = statement;
//            this.connection.setAutoCommit(false);
    }

//        public Connection getConnection() {
//            return connection;
//        }
//
//        public PreparedStatement getStatement() {
//            return statement;
//        }

    /**
     * {@inheritDoc}
     */
    public void close(TaskAttemptContext context) throws IOException {
        try {
            statement.executeBatch();
//                connection.commit();
        } catch (SQLException e) {
//                try {
//                    connection.rollback();
//                } catch (SQLException ex) {
//                    LOG.warn(StringUtils.stringifyException(ex));
//                }
            throw new IOException(e.getMessage());
        } finally {
            try {
                statement.close();
                connection.close();
            } catch (SQLException ex) {
                throw new IOException(ex.getMessage());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void write(K key, V value) throws IOException {
        try {
            key.write(statement);
            statement.addBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
