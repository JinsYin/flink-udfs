package cn.guruguru.flink.udf.scalar;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class JsonToMapTest {
    @Test
    public void testNestedMap() throws JsonProcessingException {
        String json = "{\"a\":\"1\",\"b\":{\"b1\":\"2\"}}";
        Map<String, String> map = new JsonToMap().eval(json);
        Assert.assertEquals(map.get("a"), "1");
        //Assert.assertEquals(map.get("b"), "{\"b1\":\"2\"}"); // not "{x=y}"
    }

    @Test
    public void testTypedMap() throws JsonProcessingException {
        String json = "{\"a\":\"1\",\"b\":2,\"c\":null,\"d\":\"4\",\"\":\"\"}";
        Map<String, String> map = new JsonToMap().eval(json);
        Assert.assertEquals(map.get("b"), "2");
        Assert.assertNull(map.get("c"));
        Assert.assertEquals(map.get("d"), "4");
    }
}
