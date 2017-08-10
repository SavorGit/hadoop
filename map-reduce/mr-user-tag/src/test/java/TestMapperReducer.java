import com.littlehotspot.hadoop.mr.user.tag.hbase.TagIntoHbaseMain;
import com.littlehotspot.hadoop.mr.user.tag.hbase.UserTagScheduler;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-09 上午 11:34.
 */
public class TestMapperReducer {
    @Test
    public void test(){
        String[] args = {
//                "hdfsCluster=hdfs://devpd1:8020",
                "hdfsIn=hdfs://devpd1:8020/home/data/hadoop/flume/test_user_tag",
                "hdfsOut=hdfs://devpd1:8020/home/data/hadoop/flume/test_user_tag1",

//                "hbaseRoot=hdfs://devpd1:8020/hbase",
//                "hbaseZookeeper=devpd1",
//
//                "hbaseSharePath=/user/oozie/share/lib/lib_20170601134717/hbase"
        };
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");
        Configuration conf = new Configuration();

        try {
            ToolRunner.run(conf, new TagIntoHbaseMain(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
