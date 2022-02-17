import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.junit.Test;

import java.util.HashMap;

/**
 * @Author XieChuangJian
 * @Date 2022/1/18
 */
public class testFastJSON {
    @Test
    public void test1(){
        HashMap<String, Float> map = new HashMap<>();
        map.put("num", (float) 1.66);
        System.out.println(JSON.toJSONString(map,SerializerFeature.WriteMapNullValue));
    }
}
