package org.apache.flink.connector.redis;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

public class RedisTableSource implements
// BatchTableSource<Row>, 
// ProjectableTableSource<Row>,
		StreamTableSource<Row>,
		LookupableTableSource<Row>
{

	private final RedisOptions redisOptions;
	private final TableSchema tableSchema;
	private final DeserializationSchema<Row> deserializationSchema;

	public RedisTableSource(RedisOptions redisOptions, TableSchema tableSchema, DeserializationSchema<Row> deserializationSchema) {
		this.redisOptions = redisOptions;
		this.tableSchema = tableSchema;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
		return env.addSource(new RedisChannelSourceFunction<>(redisOptions, deserializationSchema))
				.returns(getReturnType());
	}

	@Override
	public TypeInformation<Row> getReturnType() {
		return tableSchema.toRowType();
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
		return new RedisLookupFunction(redisOptions, getReturnType(), deserializationSchema);
	}

	@Override
	public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
		return null;
	}

	@Override
	public boolean isAsyncEnabled() {
		return false;
	}
}
