/**
 * Copyright (c) 2017, Stupid Bird and/or its affiliates. All rights reserved.
 * STUPID BIRD PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * @Project : hadoop
 * @Package : com.littlehotspot.hadoop.mr.box
 * @author <a href="http://www.lizhaoweb.net">李召(John.Lee)</a>
 * @EMAIL 404644381@qq.com
 * @Time : 15:38
 */
package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.contentdetail;

import com.littlehotspot.hadoop.mr.nginx.bean.Argument;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.HBaseHelper;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
import com.littlehotspot.hadoop.mr.nginx.mysql.MysqlCommonVariables;
import com.littlehotspot.hadoop.mr.nginx.mysql.model.*;
import com.littlehotspot.hadoop.mr.nginx.util.JSONUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.codehaus.jettison.json.JSONException;

import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

/**
 * 手机日志
 */
public class ConDetail extends Configured implements Tool {

    private String contents;

    private String categorys;


    private static class MobileMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
            /**数据清洗=========开始*/
            try {
                String row = Bytes.toString(result.getRow());
                String contentId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("content_id")));
                String categoryId = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("category_id")));
                String commonValue = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("common_value")));
                String timestamps = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("timestamps")));
                String optionType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("option_type")));
                String uuid = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("uuid")));
                String mediaType = Bytes.toString(result.getValue(Bytes.toBytes("attr"), Bytes.toBytes("media_type")));
                String date = stampToDate(timestamps);
                SourceBean sourceBean = new SourceBean();
                sourceBean.setUuid(uuid);
                sourceBean.setContentId(contentId);
                sourceBean.setCategoryId(categoryId);
                sourceBean.setCommonValue(commonValue);
                sourceBean.setTimestamps(timestamps);
                sourceBean.setOptionType(optionType);
                sourceBean.setMediaType(mediaType);
                sourceBean.setDateTime(date);


                if (!StringUtils.isBlank(contentId)){
                    context.write(new Text(contentId+"|"+categoryId+"|"+date), new Text(sourceBean.rowLine1()));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public static String stampToDate(String s){
            String res;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            return res;
        }
    }

    private static class MobileReduce extends Reducer<Text, Text, Text, Text> {

        private HBaseHelper hBaseHelper;

        private Map<String, Object> contentMap = new ConcurrentHashMap<>();

        private Map<String, Object> categoryMap = new ConcurrentHashMap<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.hBaseHelper = new HBaseHelper(conf);

            String contents = conf.get("contents");
            String categorys = conf.get("categorys");


            List<Object> contentList = JSONUtil.JSONArrayToList(contents, Content.class);
            for (Object o : contentList) {
                Content content = (Content) o;
                this.contentMap.put(String.valueOf(content.getId()), content);
            }

            List<Object> categoryList = JSONUtil.JSONArrayToList(categorys, Category.class);
            for (Object o : categoryList) {
                Category category = (Category) o;
                this.categoryMap.put(String.valueOf(category.getId()), category);
            }

        }
        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            try {

                Configuration conf = context.getConfiguration();
                Iterator<Text> textIterator = value.iterator();
                Integer readCount =0;
                Integer shareCount =0;
                Integer pvCount =0;
                Integer clickCount =0;
                Integer vvCount =0;
                Integer outlineCount =0;
                SourceBean bean = new SourceBean();
                while (textIterator.hasNext()) {
                    Text item = textIterator.next();
                    if (item == null) {
                        continue;
                    }
                    String rowLineContent = item.toString();
                    Matcher matcher =CommonVariables.MAPPER_LOG_FORMAT_REGEX.matcher(rowLineContent);
                    if (!matcher.find()){
                        return;
                    }
                    if (StringUtils.isBlank(bean.getContentId())){
                        bean.setContentId(matcher.group(2));
                        Content content = readMysqlContent(matcher.group(2));
                        if (null!=content){
                            bean.setContentName(content.getTitle());
                            bean.setOperators(content.getOperators());
                            bean.setCreateTime(content.getCreate_time().toString());
                        }

                    }
                    if (StringUtils.isBlank(bean.getCategoryId())){
                        bean.setCategoryId(matcher.group(3));
                        if (matcher.group(3).equals("-1")){
                            bean.setCategoryName("热点");
                        }else if (matcher.group(3).equals("-2")){
                            bean.setCategoryName("点播");
                        }else {
                            Category category = readMysqlCategory(matcher.group(3));
                            if (null!=category){
                                bean.setCategoryName(category.getName());
                            }

                        }

                    }
                    if (StringUtils.isBlank(bean.getCommonValue())){
                        bean.setCommonValue(matcher.group(7));
                    }
                    if (StringUtils.isBlank(bean.getDateTime())){
                        bean.setDateTime(matcher.group(8));
                    }

                    if (!StringUtils.isBlank(matcher.group(4))&&!StringUtils.isBlank(matcher.group(5))&&matcher.group(4).equals("start")&&matcher.group(5).equals("content")){
                        readCount ++;
                    }
                    if (!StringUtils.isBlank(matcher.group(4))&&!StringUtils.isBlank(matcher.group(5))&&matcher.group(4).equals("share")&&matcher.group(5).equals("content")){
                        shareCount ++;
                    }
                    if (!StringUtils.isBlank(matcher.group(4))&&!StringUtils.isBlank(matcher.group(5))&&matcher.group(4).equals("show")&&matcher.group(5).equals("content")){
                        pvCount ++;
                    }
                    if (!StringUtils.isBlank(matcher.group(4))&&!StringUtils.isBlank(matcher.group(5))&&matcher.group(4).equals("click")&&matcher.group(5).equals("content")){
                        clickCount ++;
                    }
                    if (!StringUtils.isBlank(matcher.group(4))&&!StringUtils.isBlank(matcher.group(5))&&matcher.group(4).equals("start")&&matcher.group(5).equals("video")){
                        vvCount ++;
                    }
                    if (!StringUtils.isBlank(matcher.group(4))&&!StringUtils.isBlank(matcher.group(5))&&matcher.group(4).equals("click")&&matcher.group(5).equals("exturl")){
                        outlineCount ++;
                    }

                }
                bean.setReadCount(readCount.toString());
                bean.setShareCount(shareCount.toString());
                bean.setPvCount(pvCount.toString());
                bean.setClickCount(clickCount.toString());
                bean.setVvCount(vvCount.toString());
                bean.setOutlineCount(outlineCount.toString());
                context.write(new Text(bean.rowLine2()), new Text());

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        /**
         * 查询酒店信息
         * @throws Exception
         */
        public Content readMysqlContent(String contentId) throws Exception{
            if (this.contentMap == null || this.contentMap.get(contentId) == null || this.contentMap.size() <= 0) {
                return null;
            }
            return (Content) this.contentMap.get(contentId);

        }


        public Category readMysqlCategory(String categoryId) throws Exception{
            if (this.categoryMap == null || this.categoryMap.get(categoryId) == null || this.categoryMap.size() <= 0) {
                return null;
            }
            return (Category) this.categoryMap.get(categoryId);

        }

    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            CommonVariables.initMapReduce(this.getConf(), args);// 初始化 MAP REDUCE

            // 获取参数
            String hbaseSharePath = CommonVariables.getParameterValue(Argument.HBaseSharePath);
            String hdfsCluster = CommonVariables.getParameterValue(Argument.HDFSCluster);
            String hdfsOutputPath = CommonVariables.getParameterValue(Argument.OutputPath);
            String time = CommonVariables.getParameterValue(Argument.Time);

            // 查询mysql
            findByMysql();
            this.getConf().set("contents", this.contents);
            this.getConf().set("categorys", this.categorys);


            Job job = Job.getInstance(this.getConf(), ConDetail.class.getSimpleName());
            job.setJarByClass(ConDetail.class);

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



            Scan scan = new Scan();

            //设置过滤器
            List<Filter> filters= new ArrayList<Filter>();

            RegexStringComparator comp = new RegexStringComparator("^(content)|(exturl)$");
            SingleColumnValueFilter typefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                    Bytes.toBytes("mda_type"), CompareFilter.CompareOp.EQUAL,comp);
            if (!StringUtils.isBlank(time)){
                SingleColumnValueFilter timefilter = new SingleColumnValueFilter(Bytes.toBytes("attr"),
                        Bytes.toBytes("date_time"), CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes(time+"00"));
                filters.add(timefilter);
            }

            if(null==scan) {
                System.out.println("error : scan = null");
                return 1;

            }

            filters.add(typefilter);

            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
            TableMapReduceUtil.initTableMapperJob("mobile_log", scan, MobileMapper.class, Text.class, Text.class, job,false);


            /**作业输出*/
            Path outputPath = new Path(hdfsOutputPath);
            FileSystem fileSystem = FileSystem.get(URI.create(outputPath.toString()), this.getConf());
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            FileOutputFormat.setOutputPath(job, outputPath);
            job.setReducerClass(MobileReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            boolean status = job.waitForCompletion(true);
            if (!status) {
                throw new Exception("MapReduce task execute failed.........");
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    public static boolean isYesterday(long time) {
        boolean isYesterday = false;
        Date date;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            date = sdf.parse(sdf.format(new Date()));
            if (time < date.getTime() && time > (date.getTime() - 24*60*60*1000)) {
                isYesterday = true;
            }
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return isYesterday;
    }

    /**
     * 查询mysql数据库
     * @throws SQLException
     * @throws IOException
     * @throws JSONException
     */
    private void findByMysql() throws SQLException, IOException, JSONException {
        JDBCTool jdbcUtil = new JDBCTool(MysqlCommonVariables.driver, MysqlCommonVariables.dbUrl, MysqlCommonVariables.userName, MysqlCommonVariables.passwd);
        jdbcUtil.getConnection();
        try {
            findContents(jdbcUtil);
            findCategorys(jdbcUtil);
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

    private void findContents(JDBCTool jdbcUtil) throws IOException, JSONException, SQLException {
        String sql = "select id,title,operators,create_time from savor_mb_content";
        List<Content> result = jdbcUtil.findResult(Content.class, sql);
        this.contents = JSONUtil.listToJsonArray(result).toString();
    }

    private void findCategorys(JDBCTool jdbcUtil) throws IOException, JSONException, SQLException {
        String sql = "select id,name from savor_mb_category";
        List<Category> result = jdbcUtil.findResult(Category.class, sql);
        this.categorys = JSONUtil.listToJsonArray(result).toString();
    }


}
