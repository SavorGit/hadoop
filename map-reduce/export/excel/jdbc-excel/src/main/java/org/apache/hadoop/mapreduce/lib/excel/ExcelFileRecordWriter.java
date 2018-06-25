/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : org.apache.hadoop.mapreduce.lib.excel
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 10:37
 */
package org.apache.hadoop.mapreduce.lib.excel;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.poi.hssf.usermodel.*;
import org.apache.poi.ss.usermodel.RichTextString;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A RecordWriter that writes the reduce output to a Excel file
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
public class ExcelFileRecordWriter<K, V> extends RecordWriter<K, V> {
    protected HSSFWorkbook writableWorkbook = null;
    protected HSSFSheet sheet = null;
    protected DataInputStream inputStream;
    protected DataOutputStream outputStream;
    protected Pattern dataPattern;

    private int rowIndex = 0;

    public ExcelFileRecordWriter(DataInputStream inputStream, DataOutputStream outputStream, String sheetName, Pattern dataPattern, String[] titles, boolean append) throws IOException {
        if (inputStream == null) {
            writableWorkbook = new HSSFWorkbook();
        } else {
            writableWorkbook = new HSSFWorkbook(inputStream);
        }
        sheet = writableWorkbook.getSheet(sheetName);
        if (sheet == null) {// 如果 Sheet 不存在，则创建。同时生成标题
            sheet = writableWorkbook.createSheet(sheetName);
            if (titles != null && titles.length > 0) {
                HSSFRow titleRow = sheet.createRow(rowIndex);
                for (int index = 0; index < titles.length; index++) {
                    HSSFCell titleCell = titleRow.createCell(index);
                    RichTextString titleRichTextString = new HSSFRichTextString(titles[index]);
                    titleCell.setCellValue(titleRichTextString);
                }
                rowIndex++;
            }
        } else if (!append) {
            if (titles != null && titles.length > 0) {
                HSSFRow titleRow = sheet.createRow(rowIndex);
                for (int index = 0; index < titles.length; index++) {
                    HSSFCell titleCell = titleRow.createCell(index);
                    RichTextString titleRichTextString = new HSSFRichTextString(titles[index]);
                    titleCell.setCellValue(titleRichTextString);
                }
                rowIndex++;
            }
        }
        if (append) {
            rowIndex = sheet.getPhysicalNumberOfRows();
        }
        this.dataPattern = dataPattern;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }

    /**
     * {@inheritDoc}
     */
    public void close(TaskAttemptContext context) throws IOException {
        writableWorkbook.write(outputStream);
        outputStream.flush();
        IOUtils.closeQuietly(outputStream);
        IOUtils.closeQuietly(inputStream);
    }

    /**
     * {@inheritDoc}
     */
    public void write(K key, V value) throws IOException {
        boolean nullValue = value == null || value instanceof NullWritable;
        if (nullValue) {
            return;
        }
        String valueData = value.toString();
        Matcher matcher = dataPattern.matcher(valueData);
        if (!matcher.find()) {
            return;
        }
        int colCount = matcher.groupCount();
        HSSFRow dataRow = sheet.createRow(rowIndex);
        for (int index = 0; index < colCount; index++) {
            String cell = matcher.group(index + 1);
            HSSFCell dataCell = dataRow.createCell(index);
            RichTextString dataRichTextString = new HSSFRichTextString(cell);
            dataCell.setCellValue(dataRichTextString);
        }
        rowIndex++;
    }
}
