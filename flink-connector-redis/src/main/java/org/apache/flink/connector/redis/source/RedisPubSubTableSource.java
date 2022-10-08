package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.connector.redis.options.RedisDeployMode.SHARDED;

/**
 * RedisPubSubTableSource for standard input stream.
 */
public class RedisPubSubTableSource implements ScanTableSource {

	private final DataType producedDataType;
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private final RedisOptions redisOptions;

	public RedisPubSubTableSource(
			DataType producedDataType, 
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			RedisOptions redisOptions) {
		this.producedDataType = producedDataType;
		this.decodingFormat = decodingFormat;
		this.redisOptions = redisOptions;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> deserializationSchema = decodingFormat.createRuntimeDecoder(
				runtimeProviderContext, producedDataType);
		
		if (redisOptions.deployMode() == SHARDED) {
			final RedisPubSubParallelSourceFunction<RowData> sourceFunction = new RedisPubSubParallelSourceFunction<>(
					redisOptions, deserializationSchema);
			return SourceFunctionProvider.of(sourceFunction, false);
		} else {
			final RedisPubSubSourceFunction<RowData> sourceFunction = new RedisPubSubSourceFunction<>(
					redisOptions, deserializationSchema);
			return SourceFunctionProvider.of(sourceFunction, false);
		}
	}

	@Override
	public DynamicTableSource copy() {
		return new RedisPubSubTableSource(producedDataType, decodingFormat, redisOptions);
	}

	@Override
	public String asSummaryString() {
		return RedisPubSubTableSource.class.getSimpleName();
	}
}
