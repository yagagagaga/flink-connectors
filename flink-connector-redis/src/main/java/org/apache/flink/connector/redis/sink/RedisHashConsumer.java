package org.apache.flink.connector.redis.sink;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

import redis.clients.jedis.PipelineBase;

public class RedisHashConsumer implements RedisRecordConsumer<RowData> {

	private final RedisRecordConsumer<RowData> delegate;

	public RedisHashConsumer(boolean isStringType, boolean isMapStructure) {
		final KeySelector<RowData, byte[]> keySelector = (isStringType)
				? r -> r.getString(0).toBytes()
				: r -> r.getBinary(0);

		if (isMapStructure) {
			delegate = (record, client) -> {
				final MapData map = record.getMap(1);
				final ArrayData fieldArray = map.keyArray();
				final ArrayData valueArray = map.valueArray();
				final int size = map.size();

				final byte[] key = keySelector.apply(record);
				for (int i = 0; i < size; i++) {
					client.hset(key, fieldArray.getBinary(i), valueArray.getBinary(i));
				}
			};
		} else {
			final ValueSelector<RowData, byte[]> fieldSelector = isStringType
					? r -> r.getString(1).toBytes()
					: r -> r.getBinary(1);
			final ValueSelector<RowData, byte[]> valueSelector = isStringType
					? r -> r.getString(2).toBytes()
					: r -> r.getBinary(2);
			delegate = (record, client) -> 
					client.hset(keySelector.apply(record), fieldSelector.apply(record), valueSelector.apply(record));
		}
	}

	@Override
	public void apply(RowData record, PipelineBase client) {
		delegate.apply(record, client);
	}
}
