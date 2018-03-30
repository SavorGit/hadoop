/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.export.excel.ord
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 19:16
 */
package com.littlehotspot.hadoop.mr.export.excel.ord;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.poi.hssf.usermodel.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * <h1>Reducer - 开机率明细</h1>
 *
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年03月30日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class OpeningRateDetailReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

    private static final Gson GSON = new Gson();

    private Map<String, ExcelConfigSheet> excelSheetConfigMap;
    //    private String workbookURL;
    HSSFWorkbook writableWorkbook = null;
    FileSystem fileSystem = null;
    OutputStream outputStream = null;
    Path workbookPath = null;

    //    @Override
//    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        String hdfsDir = null;
//        HSSFWorkbook writableWorkbook = null;
//
//        FileSystem fileSystem = null;
//        Path workbookPath = null;
//        Path workbookPathTemp = null;
//        OutputStream outputStream = null;
//        InputStream inputStream = null;
//        try {
//            hdfsDir = key.toString();
//            ExcelConfigSheet excelConfigSheet = excelSheetConfigMap.get(hdfsDir);
//
//
//            fileSystem = FileSystem.get(new URI(workbookURL), context.getConfiguration());
//            workbookPath = new Path(workbookURL);
//            workbookPathTemp = new Path(workbookURL + ".tmp");
//            if (fileSystem.exists(workbookPath)) {
//                inputStream = fileSystem.open(workbookPath);
//            }
//            outputStream = fileSystem.create(workbookPathTemp);
//
//            if (inputStream == null) {
//                writableWorkbook = new HSSFWorkbook();
//            } else {
//                writableWorkbook = new HSSFWorkbook(inputStream);
//            }
//
//            HSSFSheet sheet = writableWorkbook.getSheet(excelConfigSheet.getName());
//            if (sheet == null) {
//                sheet = writableWorkbook.createSheet(excelConfigSheet.getName());
//            }
//
//            int rowIndex = 0, colIndex = 0;
//            String[] titles = excelConfigSheet.getTitles().split(excelConfigSheet.getTitleRegexSeparator());
//
//            HSSFRow titleRow = sheet.createRow(rowIndex);
//            for (String title : titles) {
//                HSSFCell titleCell = titleRow.createCell(colIndex);
//                titleCell.setCellValue(new HSSFRichTextString(title));
//                colIndex++;
//            }
//            colIndex = 0;
//
//            for (Text text : values) {
//                rowIndex++;
//                HSSFRow dataRow = sheet.createRow(rowIndex);
//                String[] data = text.toString().split(excelConfigSheet.getFieldRegexSeparator());
//                for (String cell : data) {
//                    HSSFCell dataCell = dataRow.createCell(colIndex);
//                    dataCell.setCellValue(new HSSFRichTextString(cell));
//                    colIndex++;
//                }
//                colIndex = 0;
//            }
//
//            writableWorkbook.write(outputStream);
//            outputStream.flush();
//        } catch (URISyntaxException e) {
//            throw new IOException(e);
//        } finally {
//            IOUtils.closeQuietly(outputStream);
//            IOUtils.closeQuietly(inputStream);
//            System.out.println(hdfsDir);
//
//            if (fileSystem != null) {
//                if (fileSystem.exists(workbookPath)) {
//                    fileSystem.delete(workbookPath, true);
//                }
//                boolean success = fileSystem.rename(workbookPathTemp, workbookPath);
//                System.out.println(success);
//            }
//        }
//    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String hdfsDir = key.toString();
        ExcelConfigSheet excelConfigSheet = excelSheetConfigMap.get(hdfsDir);

        HSSFSheet sheet = writableWorkbook.getSheet(excelConfigSheet.getName());
        if (sheet == null) {
            sheet = writableWorkbook.createSheet(excelConfigSheet.getName());
        }

        int rowIndex = 0, colIndex = 0;
        String[] titles = excelConfigSheet.getTitles().split(excelConfigSheet.getTitleRegexSeparator());

        HSSFRow titleRow = sheet.createRow(rowIndex);
        for (String title : titles) {
            HSSFCell titleCell = titleRow.createCell(colIndex);
            titleCell.setCellValue(new HSSFRichTextString(title));
            colIndex++;
        }
        colIndex = 0;

        for (Text text : values) {
            rowIndex++;
            HSSFRow dataRow = sheet.createRow(rowIndex);
            String[] data = text.toString().split(excelConfigSheet.getFieldRegexSeparator());
            for (String cell : data) {
                HSSFCell dataCell = dataRow.createCell(colIndex);
                dataCell.setCellValue(new HSSFRichTextString(cell));
                colIndex++;
            }
            colIndex = 0;
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("Export excel-file ...");
        try {
            Configuration configuration = context.getConfiguration();

            String operationModeJson = configuration.get(Constants.ConfigurationKey.LIST_OPERATION_MODE);
            excelSheetConfigMap = GSON.fromJson(operationModeJson, new TypeToken<Map<String, ExcelConfigSheet>>() {
            }.getType());

//        workbookURL = configuration.get(Constants.ConfigurationKey.EXCEL_WORKBOOK);
            String workbookURL = configuration.get(Constants.ConfigurationKey.EXCEL_WORKBOOK);
            fileSystem = FileSystem.get(new URI(workbookURL), context.getConfiguration());
            workbookPath = new Path(workbookURL);
            outputStream = fileSystem.create(workbookPath);
            writableWorkbook = new HSSFWorkbook();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            writableWorkbook.write(outputStream);
            outputStream.flush();
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
        System.out.println("Export excel-file complete");
    }
}
