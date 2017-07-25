package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.user.sad;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorBox;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorHotel;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorMedia;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.SavorRoom;
import com.littlehotspot.hadoop.mr.nginx.util.JSONUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;

/**
 * <h1> 用户行为最终处理 </h1>
 * Created by Administrator on 2017-06-27 下午 3:05.
 */
public class UserSadScheduler extends Configured implements Tool {

    private String hotels;

    private String rooms;

    private String boxes;

    private String medias;

    @Override
    public int run(String[] args) throws Exception {
        try {
            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);

            String hdfsInputStart = CommonVariables.getParameterValue(Argument.InputPathStart);
            String hdfsInputEnd = CommonVariables.getParameterValue(Argument.InputPathEnd);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);

            String sadType = CommonVariables.getParameterValue(Argument.SadType);
            String hbaseRoot = CommonVariables.getParameterValue(Argument.HbaseRoot);
            String hbaseZoo = CommonVariables.getParameterValue(Argument.HbaseZookeeper);
            this.getConf().set("sadType", sadType);
            this.getConf().set("hbase.rootdir", hbaseRoot);
            this.getConf().set("hbase.zookeeper.quorum", hbaseZoo);

            // 查询mysql
            findByMysql();
            this.getConf().set("hotels", this.hotels);
            this.getConf().set("rooms", this.rooms);
            this.getConf().set("boxes", this.boxes);
            this.getConf().set("medias", this.medias);


            Path inputPath = new Path(hdfsInputStart);
            Path inputPath1 = new Path(hdfsInputEnd);
            Path outputPath = new Path(hdfsOutputPath);

            Job job = Job.getInstance(this.getConf(), this.getClass().getName());
            job.setJarByClass(this.getClass());

            // 避免报错：ClassNotFoundError hbaseConfiguration
            Configuration jobConf = job.getConfiguration();
            FileSystem hdfs = FileSystem.get(new URI(hdfsCluster), jobConf);
            Path hBaseSharePath = new Path(hbaseSharePath);
            FileStatus[] hBaseShareJars = hdfs.listStatus(hBaseSharePath);
            for (FileStatus fileStatus : hBaseShareJars) {
                if (!fileStatus.isFile()) {
                    continue;
                }
                Path archive = fileStatus.getPath();
                FileSystem fs = archive.getFileSystem(jobConf);
                DistributedCache.addArchiveToClassPath(archive, jobConf, fs);
            }//

            job.setMapperClass(UserSadMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(UserSadReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileInputFormat.addInputPath(job, inputPath1);
            FileOutputFormat.setOutputPath(job, outputPath);

            FileSystem fileSystem = FileSystem.get(new URI(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            // 执行任务
            boolean state = job.waitForCompletion(true);
            if (!state) {
                throw new Exception("MapReduce task execute failed.........");
            }

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    public void findByMysql() throws SQLException, IOException, JSONException {
        JDBCTool jdbcUtil = new JDBCTool(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
        jdbcUtil.getConnection();
        try {
            findHotels(jdbcUtil);
            findRooms(jdbcUtil);
            findBoxes(jdbcUtil);
            findMedias(jdbcUtil);
        } catch (SQLException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (JSONException e) {
            throw e;
        } finally {
            jdbcUtil.releaseConnection();
        }
    }

    private void findHotels(JDBCTool jdbcUtil) throws IOException, JSONException, SQLException {
        String sql = "select id,name from savor_hotel";
        List<SavorHotel> result = jdbcUtil.findResult(SavorHotel.class, sql);
        this.hotels = JSONUtil.listToJsonArray(result).toString();
    }

    private void findRooms(JDBCTool jdbcUtil) throws IOException, JSONException, SQLException {
        String sql = "select id,name from savor_room";
        List<SavorRoom> result = jdbcUtil.findResult(SavorRoom.class, sql);
        this.rooms = JSONUtil.listToJsonArray(result).toString();
    }

    private void findBoxes(JDBCTool jdbcUtil) throws IOException, JSONException, SQLException {
        String sql = "select id,name,mac from savor_box";
        List<SavorBox> result = jdbcUtil.findResult(SavorBox.class, sql);
        this.boxes = JSONUtil.listToJsonArray(result).toString();
    }

    private void findMedias(JDBCTool jdbcUtil) throws IOException, JSONException, SQLException {
        String sql = "select id,name,oss_addr from savor_media";
        List<SavorMedia> result = jdbcUtil.findResult(SavorMedia.class, sql);
        this.medias = JSONUtil.listToJsonArray(result).toString();
    }
}