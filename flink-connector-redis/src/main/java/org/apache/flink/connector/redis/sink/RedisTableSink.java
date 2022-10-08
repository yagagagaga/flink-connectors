package org.apache.flink.connector.redis.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * {@link #RedisTableSink} for Redis.
 */
public class RedisTableSink implements DynamicTableSink {

	private final DataType physicalDataType;
	private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
	private final RedisOptions redisOptions;

	public RedisTableSink(
			DataType physicalDataType,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat,
			RedisOptions redisOptions) {
		this.physicalDataType = physicalDataType;
		this.encodingFormat = encodingFormat;
		this.redisOptions = redisOptions;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return ChangelogMode.insertOnly();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		SerializationSchema<RowData>  serializationSchema = encodingFormat != null
				? encodingFormat.createRuntimeEncoder(context, physicalDataType)
				: null;
		final RedisRecordConsumer<RowData> consumer = redisOptions.createRecordConsumer(physicalDataType, serializationSchema);
		final TypeInformation<RowData> typeInfo = context.createTypeInformation(physicalDataType);
		
		return SinkFunctionProvider.of(new RedisSinkFunction<>(typeInfo, consumer, redisOptions));
	}

	@Override
	public DynamicTableSink copy() {
		return new RedisTableSink(physicalDataType, encodingFormat, redisOptions);
	}

	@Override
	public String asSummaryString() {
		return RedisTableSink.class.getSimpleName();
	}
}
