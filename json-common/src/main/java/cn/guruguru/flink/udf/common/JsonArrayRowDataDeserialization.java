package cn.guruguru.flink.udf.common;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

/**
 * Deserialize a json array string to a row
 * <p>SqlValidatorException thrown when select from a view which contains a UDTF call
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-18750">SqlValidatorException thrown
 * when select from a view which contains a UDTF call</a>
 */
@Public
public class JsonArrayRowDataDeserialization {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    // ~ Collector mode -----------------------------------

    /**
     * Deserialize a JSON array string to a set of {@link Row}
     *
     * @param messages JSON array string to be deserialized
     * @param dataType data ype
     * @param out collector
     */
    public void deserialize(byte[] messages, DataType dataType, Collector<Row> out) throws IOException {
        deserialize(
                messages,
                dataType,
                false,
                true,
                true,
                out);
    }

    /**
     * Deserialize a JSON array string to a set of {@link Row}
     *
     * @param messages JSON array string to be deserialized
     * @param dataType data type to be deserialized
     * @param failOnMissingField fail on missing field
     * @param ignoreParseErrors ignore parser errors
     * @param acceptSingleValueAsArray accept single value as array
     * @param out collector
     */
    public void deserialize(byte[] messages,
                            DataType dataType,
                            boolean failOnMissingField,
                            boolean ignoreParseErrors,
                            boolean acceptSingleValueAsArray,
                            Collector<Row> out) throws IOException {
        deserialize(
                messages,
                dataType,
                failOnMissingField,
                ignoreParseErrors,
                acceptSingleValueAsArray).forEach(out::collect);
    }

    // Return mode ----------------------------------------

    /**
     * Deserialize a JSON array string to a set of {@link Row}
     *
     * @param messages JSON array string to be deserialized
     * @param dataType data ype
     * @return a list of {@link Row}
     */
    public List<Row> deserialize(byte[] messages, DataType dataType) throws IOException {
        return deserialize(
                messages,
                dataType,
                false,
                true,
                true);
    }

    /**
     * Deserialize a JSON array string to a set of {@link Row}
     *
     * @param messages JSON array string to be deserialized
     * @param dataType data type to be deserialized
     * @param failOnMissingField fail on missing field
     * @param ignoreParseErrors ignore parser errors
     * @param acceptSingleValueAsArray accept single value as array
     * @return a list of {@link Row}
     */
    public List<Row> deserialize(byte[] messages,
                            DataType dataType,
                            boolean failOnMissingField,
                            boolean ignoreParseErrors,
                            boolean acceptSingleValueAsArray) throws IOException {
        RowType rowType = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo =
                TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo(rowType);
        List<Row> rowList = new LinkedList<>();
        objectMapper.configure(
                DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY,
                acceptSingleValueAsArray);
        JsonRowDataDeserializationSchema deserializer = new JsonRowDataDeserializationSchema(
                rowType,
                resultTypeInfo,
                failOnMissingField,
                ignoreParseErrors,
                TimestampFormat.ISO_8601
        );
        try {
            // deserialize a string array to an object array, this may throw some exceptions
            Object[] objects = objectMapper.readValue(messages, Object[].class);
            for (Object object : objects) {
                // serialize an object to a json string with byte[] type,
                // this may throw a JsonProcessingException
                byte[] message = objectMapper.writeValueAsBytes(object);
                // deserialize a json string to a RowData, this may throw a IOException
                RowData rowData = deserializer.deserialize(message);
                // convert to an external Row
                Row row = RowDataConverter.toExternalRow(rowData, dataType);
                rowList.add(row);
            }
        } catch (Throwable t) {
            if (!ignoreParseErrors) {
                throw new IOException(
                        format("Failed to deserialize JSON array '%s'.", new String(messages)), t);
            }
        }
        return rowList;
    }
}
