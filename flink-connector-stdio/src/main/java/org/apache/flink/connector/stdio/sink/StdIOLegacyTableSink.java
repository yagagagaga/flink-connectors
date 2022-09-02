package org.apache.flink.connector.stdio.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * StdIOLegacyTableSink for standard output stream.
 */
@Deprecated
public class StdIOLegacyTableSink implements AppendStreamTableSink<Row> {

	private final TableSchema tableSchema;

	public StdIOLegacyTableSink(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return dataStream.print().setParallelism(1);
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new StdIOLegacyTableSink(tableSchema);
	}

	@Override
	public String[] getFieldNames() {
		return tableSchema.getFieldNames();
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return tableSchema.getFieldTypes();
	}

	@Override
	public TypeInformation<Row> getOutputType() {
		return tableSchema.toRowType();
	}
}
