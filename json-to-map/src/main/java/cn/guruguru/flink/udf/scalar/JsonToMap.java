package cn.guruguru.flink.udf.scalar;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * JSON_TO_MAP scalar function
 */
public class JsonToMap extends ScalarFunction {
    private static final Map<String, String> EMPTY_MAP = new HashMap<>(0);

    /**
     * Convert a json to a map
     *
     * <p>Notes: Nested maps are not supported
     * {@see org.apache.flink.table.runtime.functions.SqlFunctionUtils#strToMap(String)}
     * @param json a json string
     * @return DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())
     */
    @SuppressWarnings("unchecked")
    @FunctionHint(output = @DataTypeHint("MAP<STRING, STRING>"))
    public Map<String, String> eval(String json) throws JsonProcessingException {
        if (StringUtils.isEmpty(json)) {
            return EMPTY_MAP;
        }
        ObjectMapper objectMapper = new ObjectMapper();
        Map<Object, Object> map = objectMapper.readValue(json, Map.class); // typeRef or Map.class
        return standardize(map);
    }

    /**
     * Convert a nested json to a nested map
     *
     * TODO: Nested Map and User Defined Map
     *
     * Usage:
     * - JsonToMap(column, 'MAP<STRING,STRING>')
     * - JsonToMap(column, 'MAP<STRING,INT>')
     * - JsonToMap(column, 'MAP<STRING, MAP<STRING,INT>>')
     * - JsonToMap(column, 'MAP<STRING, ROW<x STRING,y INT>>') - not supported
     *
     * @param nestedJson a nested json string
     * @return a nested map
     */
    public Map<String, Object> eval(String nestedJson, String typeDesc) throws JsonProcessingException {
        throw new UnsupportedOperationException("Unsupported conversion");
    }

    // ~ utilities ------------------------------------------------------------

    /**
     * Standardize a map
     *
     * @param source source map with Map<Object, Object> type
     * @return standardize map with Map<String, String> type
     */
    private Map<String, String> standardize(Map<Object, Object> source) {
        Map<String, String> target = new HashMap<>();
        source.forEach((k, v) -> {
            // convert the key object to a string, return "null" if the keyObj is NULL
            String keyStr = String.valueOf(k);
            // convert the value object to a string, return NULL if old value is NULL, not "" or "null"
            String valStr = Objects.isNull(v) ? null : v.toString();
            target.put(keyStr, valStr);
        });
        return target;
    }
}
