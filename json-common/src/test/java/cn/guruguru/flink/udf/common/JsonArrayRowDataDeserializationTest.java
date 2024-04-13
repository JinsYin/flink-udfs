package cn.guruguru.flink.udf.common;

import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.*;

/**
 * Unit test for the {@link JsonArrayRowDataDeserialization}
 */
public class JsonArrayRowDataDeserializationTest {

    /**
     * 为了模拟 {@link org.apache.flink.table.functions.TableFunction#collect(Object)}
     * 重命名 {@link org.apache.flink.table.functions.TableFunction#collect()} 方法，
     * 避免与 {@link Collector#Collect() } 方法重名，导致递归调用
     */
    protected final void anotherCollect(Row row) {
        System.out.println(row);
    }

    // ------------------ Collector Mode ------------------

    @Test
    public void testDeserWithCollector() throws IOException {
        String jsonArray = "[{\"name\": \"alice\", \"age\": 20},{\"name\": \"bob\", \"age\": 18}]";
        DataType dataType = ROW(
                FIELD("name", STRING()),
                FIELD("age", INT())
        );
        byte[] messages = jsonArray.getBytes();
        new JsonArrayRowDataDeserialization().deserialize(messages, dataType, new Collector<Row>() {
            @Override
            public void collect(Row row) {
                // collect(row); // java.lang.StackOverflowError
                anotherCollect(row);
            }
            @Override
            public void close() {}
        });
    }

    // -------------------- Return Mode -------------------

    @Test
    public void testDeser() throws IOException {
        String jsonArray = "[{\"name\": \"alice\", \"age\": 20},{\"name\": \"bob\", \"age\": 18}]";
        DataType dataType = ROW(
                FIELD("name", STRING()),
                FIELD("age", INT())
        );
        byte[] messages = jsonArray.getBytes();
        List<Row> rowList = new JsonArrayRowDataDeserialization().deserialize(messages, dataType);
        rowList.forEach(this::anotherCollect);
    }
}
