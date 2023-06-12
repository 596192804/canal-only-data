import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

/**
 * @Author XieChuangJian
 * @Date 2022/3/11
 */
public class test {
    @Test
    public void test1(){
        String[] tmp={"a","b"};
        System.out.println(ArrayUtils.contains(tmp,"ab"));
    }
}
