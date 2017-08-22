import com.littlehotspot.hadoop.web.service.ITestService;
import com.littlehotspot.hadoop.web.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.Date;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-17 下午 4:46.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:applicationContext.xml"})
public class TestClass {

    @Autowired
    ITestService testService;

    @Test
    public void test() {
        System.setProperty("hadoop.home.dir", "E:\\GreenProfram\\HadoopEcosphere\\applications\\hadoop2.6_x64-for-win");

        Configuration conf = new Configuration();

        HTable hTable = null;
        try {
            hTable = new HTable(conf, "user_date");

            long pageSize = 1;
            int totalRowsCount = 0;
            PageFilter filter = new PageFilter(pageSize);
            byte[] lastRow = null;
//            while(true) {
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("222"));
            scan.setStopRow(Bytes.toBytes("444"));
            scan.setFilter(filter);

//                if(lastRow != null) {
//                    byte[] postfix = Bytes.toBytes("postfix");
//                    byte[] startRow = Bytes.add(lastRow, postfix);
//                    scan.setStartRow(startRow);
//                    System.out.println("start row : " + Bytes.toString(startRow));
//                }

            ResultScanner scanner = hTable.getScanner(scan);
            int localRowsCount = 0;
            for (Result result : scanner) {
                System.out.println(localRowsCount++ + " : " + result);
                totalRowsCount++;
                lastRow = result.getRow(); // ResultScanner 的结果集是排序好的，这样就可以取到最后一个 row 了
                System.out.println(Bytes.toString(lastRow));
                System.out.println(lastRow.equals("222".getBytes()));
            }
            scanner.close();

//                if(localRowsCount == 0) break;
//            }
            System.out.println("total rows is : " + totalRowsCount);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testTableRowCount() {
        long time = new Date().getTime();
        String coprocessorClassName = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";
        HBaseUtil.addTableCoprocessor("user_date", coprocessorClassName);
        long rowCount = HBaseUtil.rowCount("user_date", "attr");
        System.out.println("rowCount: " + rowCount);

        System.out.println(new Date().getTime() - time);

        time = new Date().getTime();
        long row = HBaseUtil.rowCount("user_date");
        System.out.println("rowCount: " + row);
        System.out.println(new Date().getTime() - time);
    }

}
