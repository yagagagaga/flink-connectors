package org.apache.flink.connector.redis;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.types.Row;

/**
 * UpsertTableSink for Redis.
 */
public class RedisUpsertTableSink implements UpsertStreamTableSink<Row> {

	private final RedisOptions redisOptions;
	private final TableSchema tableSchema;
	private final SerializationSchema<Row> serializationSchema;

	private final TypeInformation<Row> recordType;
	private final String[] keyFields;
	private final TypeInformation<?>[] fieldTypes;

	public RedisUpsertTableSink(RedisOptions redisOptions, TableSchema tableSchema, SerializationSchema<Row> serializationSchema) {
		this.redisOptions = redisOptions;
		this.tableSchema = tableSchema;
		this.serializationSchema = serializationSchema;

		this.keyFields = tableSchema.getFieldNames();
		this.fieldTypes = tableSchema.getFieldTypes();
		this.recordType = tableSchema.toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public void setKeyFields(String[] keys) {
		// do nothing
	}

	@Override
	public void setIsAppendOnly(Boolean isAppendOnly) {
		// do nothing
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return recordType;
	}

	@Override
	public String[] getFieldNames() {
		return keyFields;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		return dataStream.addSink(new RedisRetractSinkFunction(redisOptions, serializationSchema))
				.name(getClass().getName() + "(field, value)");
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return new RedisUpsertTableSink(redisOptions, tableSchema, serializationSchema);
	}
}
