package cn.guruguru.flink.udf.common;

import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.IOException;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;

/**
 * Unit test for the {@link JsonRowDataDeserialization}
 */
public class JsonRowDataDeserializationTest {

    @Test
    public void testDeser() throws IOException {
        String jsonArray = "{\"name\": \"alice\", \"age\": 20}";
        DataType dataType = ROW(
                FIELD("name", STRING()),
                FIELD("age", INT())
        );
        byte[] messages = jsonArray.getBytes();
        Row row = new JsonRowDataDeserialization().deserialize(messages, dataType);
        System.out.println(row);
    }
}
