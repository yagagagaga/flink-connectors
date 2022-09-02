package org.apache.flink.connector.stdio.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

/**
 * StdIOLegacyTableSource for standard input stream.
 */
@Deprecated
public class StdIOLegacyTableSource implements StreamTableSource<Row> {

	private final TableSchema tableSchema;
	private final DeserializationSchema<Row> deserializationSchema;

	public StdIOLegacyTableSource(TableSchema tableSchema, DeserializationSchema<Row> deserializationSchema) {
		this.tableSchema = tableSchema;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return getTableSchema().toRowType();
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		return env
				.addSource(new StdIOSourceFunction<>(deserializationSchema))
				.returns(getReturnType());
	}
}
