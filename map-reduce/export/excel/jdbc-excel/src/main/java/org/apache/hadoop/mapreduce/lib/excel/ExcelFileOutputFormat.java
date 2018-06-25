/**
 * Copyright (c) 2018, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : org.apache.hadoop.mapreduce.lib.excel
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 18:14
 */
package org.apache.hadoop.mapreduce.lib.excel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author <a href="http://www.lizhaoweb.cn">李召(John.Lee)</a>
 * @version 1.0.0.0.1
 * @EMAIL 404644381@qq.com
 * @notes Created on 2018年06月21日<br>
 * Revision of last commit:$Revision$<br>
 * Author of last commit:$Author$<br>
 * Date of last commit:$Date$<br>
 */
public class ExcelFileOutputFormat<K, V> extends FileOutputFormat<K, V> {
    //    public static final String OUTFILE = "mapreduce.output.fileoutputformat.outputfile";
    public static final String SHEET_NAME = "mapreduce.output.exceloutput.sheet.name";
    public static final String TITLES = "mapreduce.output.exceloutput.sheet.titles";
    public static final String DATA_PATTERN = "mapreduce.output.exceloutput.sheet.data.pattern";

    private static final String FILE_SUFFIX = ".xls";


    /**
     * {@inheritDoc}
     */
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String sheetName = conf.get(ExcelFileOutputFormat.SHEET_NAME, "MR Sheet");
        String[] titles = conf.getStrings(ExcelFileOutputFormat.TITLES);
        Pattern dataPattern = conf.getPattern(ExcelFileOutputFormat.DATA_PATTERN, null);
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
            codec = ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path workFile = getDefaultWorkFile(job, extension);
        Path excelFile = getOutputPath(job);
        FileSystem fs = workFile.getFileSystem(conf);
        FSDataInputStream fileIn = null;
        if (fs.exists(excelFile)) {
            fileIn = fs.open(excelFile);
        }
        FSDataOutputStream fileOut = fs.create(workFile, false);
        if (!isCompressed) {
            return new ExcelFileRecordWriter<>(fileIn, fileOut, sheetName, dataPattern, titles, false);
        } else {
            DataInputStream dataInputStream = fileIn == null ? null : new DataInputStream(codec.createInputStream(fileIn));
            DataOutputStream dataOutputStream = fileOut == null ? null : new DataOutputStream(codec.createOutputStream(fileOut));
            return new ExcelFileRecordWriter<>(dataInputStream, dataOutputStream, sheetName, dataPattern, titles, false);
        }
    }

    /**
     * Set the {@link Path} of the output directory for the map-reduce job.
     *
     * @param job          The job to modify
     * @param outputFile   the {@link Path} of the output file for the map-reduce job.
     * @param sheetName    The name of the sheet
     * @param titles       The title in sheet
     * @param valuePattern The value to matching
     */
    public static void setOutputPath(Job job, Path outputFile, String sheetName, String[] titles, Pattern valuePattern) {
        Path outputDir = outputFile.getParent();
        try {
            outputDir = outputFile.getFileSystem(job.getConfiguration()).makeQualified(outputDir);
        } catch (IOException e) {
            // Throw the IOException as a RuntimeException to be compatible with MR1
            throw new RuntimeException(e);
        }
        job.getConfiguration().setInt(MRJobConfig.NUM_REDUCES, 1);
//        job.getConfiguration().set(ExcelFileOutputFormat.OUTFILE, outputFile.toString());
        job.getConfiguration().set(ExcelFileOutputFormat.SHEET_NAME, sheetName);
        if (titles != null) {
            job.getConfiguration().setStrings(ExcelFileOutputFormat.TITLES, titles);
        }
        job.getConfiguration().setPattern(ExcelFileOutputFormat.DATA_PATTERN, valuePattern);
        job.getConfiguration().set(ExcelFileOutputFormat.OUTDIR, outputDir.toString());
        job.getConfiguration().set(ExcelFileOutputFormat.BASE_OUTPUT_NAME, outputFile.getName());
    }

    public static void setOutputPath(Job job, Path outputFile) {
        throw new RuntimeException("Method not supported");
    }

    /**
     * Get the {@link Path} to the output directory for the map-reduce job.
     *
     * @return the {@link Path} to the output directory for the map-reduce job.
     * @see FileOutputFormat#getWorkOutputPath(TaskInputOutputContext)
     */
    public static Path getOutputPath(JobContext job) {
        Configuration conf = job.getConfiguration();
        String dirName = conf.get(FileOutputFormat.OUTDIR);
        if (dirName == null) {
            return null;
        }
        String fileName = conf.get(ExcelFileOutputFormat.BASE_OUTPUT_NAME);
        if (fileName == null) {
            return null;
        }
        String name = String.format("%s/%s%s", dirName, fileName, ExcelFileOutputFormat.FILE_SUFFIX);
        return name == null ? null : new Path(name);
    }

    /**
     * Generate a unique filename, based on the task id, name, and extension
     *
     * @param context   the task that is calling this
     * @param name      the base filename
     * @param extension the filename extension
     * @return a string like $name.xlsx
     */
    public synchronized static String getUniqueFile(TaskAttemptContext context, String name, String extension) {
        StringBuilder result = new StringBuilder();
        result.append(name);
        result.append(ExcelFileOutputFormat.FILE_SUFFIX);
        result.append(extension);
        return result.toString();
    }

    /**
     * Get the default path and filename for the output format.
     *
     * @param context   the task context
     * @param extension an extension to add to the filename
     * @return a full path $output/_temporary/$taskid/part.xlsx
     * @throws IOException
     */
    @Override
    public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        return new Path(committer.getWorkPath(), getUniqueFile(context, getOutputName(context), extension));
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws FileAlreadyExistsException, IOException {
        // Ensure that the output directory is set and not already there
        Path outFile = getOutputPath(job);
        if (outFile == null) {
            throw new InvalidJobConfException("Output file not set.");
        }

        // get delegation token for outDir's file system
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[]{outFile}, job.getConfiguration());

//        if (outFile.getFileSystem(job.getConfiguration()).exists(outFile)) {
//            throw new FileAlreadyExistsException("Output file " + outFile + " already exists");
//        }
    }
}
