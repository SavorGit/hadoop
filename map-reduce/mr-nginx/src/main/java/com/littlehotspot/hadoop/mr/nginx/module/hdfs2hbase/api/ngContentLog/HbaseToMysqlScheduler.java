package com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog;

import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.JDBCTool;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.io.ContentLogWritable;
import com.littlehotspot.hadoop.mr.nginx.module.hdfs2hbase.api.ngContentLog.io.NgContentLogHbase;
import net.lizhaoweb.common.util.argument.ArgumentFactory;
import net.lizhaoweb.spring.hadoop.commons.argument.MapReduceConstant;
import net.lizhaoweb.spring.hadoop.commons.argument.model.Argument;
import net.lizhaoweb.spring.hadoop.hbase.util.HBaseHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.sql.SQLException;
import java.util.regex.Matcher;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-09-06 下午 5:52.
 */
public class HbaseToMysqlScheduler extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        try {
            MapReduceConstant.CommonVariables.initMapReduce(this.getConf(), args);

            // 任务名称
            String jobName = ArgumentFactory.getParameterValue(Argument.JobName);
            ArgumentFactory.printInputArgument(Argument.JobName, jobName, false);

            String jdbcDriver = ArgumentFactory.getParameterValue(Argument.JDBCDriver);
            ArgumentFactory.printInputArgument(Argument.JDBCDriver, jdbcDriver, false);

            String jdbcUrl = ArgumentFactory.getParameterValue(Argument.JDBCUrl);
            ArgumentFactory.printInputArgument(Argument.JDBCUrl, jdbcUrl, false);

            String jdbcUsername = ArgumentFactory.getParameterValue(Argument.JDBCUsername);
            ArgumentFactory.printInputArgument(Argument.JDBCUsername, jdbcUsername, false);

            String jdbcPassword = ArgumentFactory.getParameterValue(Argument.JDBCPassword);
            ArgumentFactory.printInputArgument(Argument.JDBCPassword, jdbcPassword, true);

            // 准备工作
            if (StringUtils.isBlank(jobName)) {
                jobName = this.getClass().getName();
            }
            String sql = "truncate savor_ng_content_log";
            JDBCTool jdbcUtil = new JDBCTool(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);
            jdbcUtil.getConnection();
            try {
                jdbcUtil.updateByPreparedStatement(sql, null);
            } catch (SQLException e) {
                e.printStackTrace();
                return 1;
            } finally {
                jdbcUtil.releaseConnection();
            }

            // 这句话很关键
//            this.getConf().set("mapred.job.tracker", "localhost:9001");
            DBConfiguration.configureDB(this.getConf(), jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword);

            Job job = Job.getInstance(this.getConf(), jobName);
            job.setJarByClass(this.getClass());     // class that contains mapper

            Scan scan = new Scan();
//            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
//            scan.setCacheBlocks(false);  // don't set to true for MR jobs

            TableMapReduceUtil.initTableMapperJob(
                    CommonVariables.HBASE_TABLE_NAME,        // input HBase table name
                    scan,             // Scan instance to control CF and attribute selection
                    HBaseInputMapper.class,   // mapper
                    Text.class,             // mapper output key
                    Text.class,             // mapper output value
                    job
            );

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(DBOutputReducer.class);
            job.setOutputFormatClass(DBOutputFormat.class);
            DBOutputFormat.setOutput(job, "savor_ng_content_log"
                    , "ip", "is_wx", "net_type", "device_type",
                    "timestamp", "content_id", "channel", "is_sq", "request_url"
            );

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

    private static class HBaseInputMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            try {
                NgContentLogHbase ngContentLogHbase = HBaseHelper.toBean(result, NgContentLogHbase.class);
                String row = ngContentLogHbase.getAttrBean().toString();
                context.write(new Text(ngContentLogHbase.getRowKey()), new Text(row));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class DBOutputReducer extends Reducer<Text, Text, ContentLogWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String line = values.iterator().next().toString();
            Matcher matcher = CommonVariables.MYSQL_ROW_PATTERN.matcher(line);
            if (!matcher.find()) {
                return;
            }

            ContentLogWritable writable = new ContentLogWritable();
            writable.setIp(matcher.group(1));
            writable.setIsWx(matcher.group(2));
            writable.setNetType(matcher.group(3));
            writable.setDeviceType(matcher.group(4));
            writable.setTimestamp(Long.parseLong(matcher.group(5)));
            writable.setContentId(matcher.group(6));
            writable.setChannel(matcher.group(7));
            writable.setIsSq(matcher.group(8));
            writable.setRequestUrl(matcher.group(9));

            context.write(writable, new Text());
        }
    }
}
