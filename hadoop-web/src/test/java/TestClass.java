import com.littlehotspot.hadoop.web.service.ITestService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * <h1> title </h1>
 * Created by Administrator on 2017-08-17 下午 4:46.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"classpath*:applicationContext.xml"})
public class TestClass {

    @Autowired
     ITestService testService;

    @Test
    public void test() {
        System.out.println(testService.getService());
    }
}
