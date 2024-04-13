package cn.guruguru.flink.udf.common;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.io.IOException;

@Public
public class JsonRowDataDeserialization {

    /**
     * Deserialize a JSON string to a {@link Row}
     *
     * @param message json to be deserialized
     * @param dataType data type to be deserialized
     * @return a {@link Row}
     */
    public Row deserialize(byte[] message, DataType dataType) throws IOException {
        return deserialize(message, dataType, false, true);
    }

    /**
     * Deserialize a JSON string to a {@link Row}
     *
     * <p>Inspired by {@link org.apache.flink.formats.json flink-json} project
     * <p>see `org.apache.flink.formats.json.JsonRowDataSerDeSchemaTest#testSerDe`
     * @see org.apache.flink.formats.json.JsonFormatFactory#createDecodingFormat
     * @see org.apache.flink.formats.json.JsonRowDataDeserializationSchema#JsonRowDataDeserializationSchema
     * @see org.apache.flink.formats.json.JsonRowDataDeserializationSchema#deserialize
     * @see org.apache.flink.formats.json.canal.CanalJsonDeserializationSchema#deserialize
     * @see org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema#deserialize
     * @param message json to be deserialized
     * @param dataType data type to be deserialized
     * @param failOnMissingField fail on missing filed
     * @param ignoreParseErrors ignore parse errors
     */
    public Row deserialize(byte[] message,
                           DataType dataType,
                           boolean failOnMissingField,
                           boolean ignoreParseErrors) throws IOException {
        final RowType rowType = (RowType) dataType.getLogicalType();
        final TypeInformation<RowData> resultTypeInfo =
                TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo(rowType);
        JsonRowDataDeserializationSchema deserializer = new JsonRowDataDeserializationSchema(
                rowType,
                resultTypeInfo,
                failOnMissingField,
                ignoreParseErrors,
                TimestampFormat.ISO_8601);
        // Internal data structure of the 'ROW' data type
        RowData rowData = deserializer.deserialize(message);
        return RowDataConverter.toExternalRow(rowData, dataType);
    }
}
