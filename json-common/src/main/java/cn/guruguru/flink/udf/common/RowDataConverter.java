package cn.guruguru.flink.udf.common;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public final class RowDataConverter {

    /**
     * Converts internal {@link RowData} to external {@link Row}
     *
     * <p>see `org.apache.flink.formats.json.JsonRowDataSerDeSchemaTest#convertToExternal`
     * @param rowData internal {@link RowData}
     * @param dataType data type
     * @return external {@link Row}
     */
    @SuppressWarnings("unchecked")
    public static Row toExternalRow(RowData rowData, DataType dataType) {
        return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
    }

}
