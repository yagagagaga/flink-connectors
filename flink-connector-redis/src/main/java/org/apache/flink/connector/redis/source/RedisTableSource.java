package org.apache.flink.connector.redis.source;

import org.apache.flink.connector.redis.options.RedisDataType;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * RedisTableSource for Redis multi sentinel cluster.
 */
public class RedisTableSource implements ScanTableSource, LookupTableSource {

	private final DataType physicalDataType;
	private final RedisOptions redisOptions;

	public RedisTableSource(
			DataType physicalDataType,
			RedisOptions redisOptions) {
		this.physicalDataType = physicalDataType;
		this.redisOptions = redisOptions;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		final RedisRecordProducer<RowData> producer = redisOptions.createRecordProducer(physicalDataType);
		return SourceFunctionProvider.of(new RedisSourceFunction<>(producer, redisOptions), true);
	}

	@Override
	public DynamicTableSource copy() {
		return new RedisTableSource(physicalDataType, redisOptions);
	}

	@Override
	public String asSummaryString() {
		return RedisTableSource.class.getSimpleName();
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		if (redisOptions.dataType() != RedisDataType.HASH) {
			checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1,
					"Currently, Redis table can only be lookup by single key.");
		} else {
			checkArgument(context.getKeys().length <= 2,
					"Currently, Redis HASH table can only be lookup by most 2 key.");
		}

		if (redisOptions.async()) {
			final RedisRowDataAsyncLookupFunction tableFunction =
					new RedisRowDataAsyncLookupFunction(redisOptions, redisOptions.createRecordProducer(physicalDataType));
			return AsyncTableFunctionProvider.of(tableFunction);
		} else {
			final RedisRowDataLookupFunction tableFunction =
					new RedisRowDataLookupFunction(redisOptions, redisOptions.createRecordProducer(physicalDataType));
			return TableFunctionProvider.of(tableFunction);
		}
	}
}
