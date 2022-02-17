import java.io.FileInputStream;

import java.util.Properties;

import com.alibaba.otter.canal.deployer.CanalStarter;
import org.junit.Test;

public class CanalTest {

    @Test
    public void testCanal () throws Throwable {
        Properties pro = new Properties();
        FileInputStream in = new FileInputStream("src/main/resources/canal.properties");
        pro.load(in);
        CanalStarter canalStarter = new CanalStarter(pro);
        canalStarter.start();
    }
}
