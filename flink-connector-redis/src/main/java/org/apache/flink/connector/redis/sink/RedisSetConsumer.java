package org.apache.flink.connector.redis.sink;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

import redis.clients.jedis.PipelineBase;

public class RedisSetConsumer implements RedisRecordConsumer<RowData> {

	private final RedisRecordConsumer<RowData> delegate;

	public RedisSetConsumer(boolean isStringType, boolean isArrayType) {
		final KeySelector<RowData, byte[]> keySelector = isStringType
				? r -> r.getString(0).toBytes()
				: r -> r.getBinary(0);

		final ValueSelector<RowData, byte[]> valueSelector = isArrayType
				? null : isStringType
				? r -> r.getString(1).toBytes()
				: r -> r.getBinary(1);

		final ValueSelector<RowData, byte[][]> multiValueSelector = !isArrayType
				? null : isStringType
				? this::convertToString
				: this::convertToBytes;

		if (valueSelector == null) {
			delegate = (record, client) -> client.sadd(keySelector.apply(record), multiValueSelector.apply(record));
		} else {
			delegate = (record, client) -> client.sadd(keySelector.apply(record), valueSelector.apply(record));
		}
	}

	private byte[][] convertToBytes(RowData r) {
		final ArrayData array = r.getArray(1);
		final int size = array.size();
		byte[][] data = new byte[size][];
		for (int i = 0; i < size; i++) {
			data[i] = array.getBinary(i);
		}
		return data;
	}

	private byte[][] convertToString(RowData r) {
		final ArrayData array = r.getArray(1);
		final int size = array.size();
		byte[][] data = new byte[size][];
		for (int i = 0; i < size; i++) {
			data[i] = array.getString(i).toBytes();
		}
		return data;
	}

	@Override
	public void apply(RowData record, PipelineBase client) {
		delegate.apply(record, client);
	}
}
