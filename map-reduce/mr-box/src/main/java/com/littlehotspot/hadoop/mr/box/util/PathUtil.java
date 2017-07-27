package com.littlehotspot.hadoop.mr.box.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *@Author 刘飞飞
 *@Date 2017/7/7 10:21
 *
 */
public class PathUtil {
    /**
     *
     * @param conf  配置
     * @param path  指定根目录
     * @return
     */
    public static List<Path> getPathDirectoryAll(Configuration conf,String path){
        Path root=new Path(path);
        FileSystem fileSystem = null;
        List<Path> pathList=new ArrayList();
        try {
            fileSystem = FileSystem.get(new URI(path.toString()),conf);
            FileStatus[] files=fileSystem.listStatus(root);
            for(FileStatus fileStatu:files){
                if(fileStatu.isDirectory()){
                    pathList.add(fileStatu.getPath());
                }
            }
            Collections.sort(pathList);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return  pathList;
    }

    public static void deletePath(Configuration conf,Path path){
        FileSystem inFileSystem = null;
        try {
            inFileSystem = FileSystem.get(new URI(path.toString()),conf);
            if (inFileSystem.exists(path)){
                inFileSystem.delete(path,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }



}
