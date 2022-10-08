package org.apache.flink.connector.redis.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBase;

public class RedisPublicConsumer implements RedisRecordConsumer<RowData> {
	
	private final byte[] channel;
	private final SerializationSchema<RowData> serializationSchema;

	public RedisPublicConsumer(String channel, SerializationSchema<RowData> serializationSchema) {
		this.channel = channel.getBytes();
		this.serializationSchema = serializationSchema;
	}

	@Override
	public void apply(RowData record, PipelineBase client) {
		final byte[] message = serializationSchema.serialize(record);
		if (client instanceof Pipeline) {
			((Pipeline) client).publish(channel, message);
		}
		// todo how about in sharded mode?
	}
}
