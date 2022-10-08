package org.apache.flink.connector.redis.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import redis.clients.jedis.BinaryJedisPubSub;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.connector.redis.options.RedisDataType.PUBSUB;

/**
 * RedisPubSubParallelSourceFunction for standard input stream.
 */
public class RedisPubSubSinkFunction<T> extends RichSinkFunction<T> {

	private final RedisOptions redisOptions;
	private final SerializationSchema<T> serializationSchema;
	private final AtomicBoolean running = new AtomicBoolean(false);

	private RedisClientProxy client;
	private BinaryJedisPubSub pubSub;

	public RedisPubSubSinkFunction(RedisOptions redisOptions, SerializationSchema<T> serializationSchema) {
		this.redisOptions = redisOptions;
		this.serializationSchema = serializationSchema;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (!redisOptions.dataType().equals(PUBSUB)) {
			throw new IllegalArgumentException("" +
					"You must make sure your redis data type is " +
					PUBSUB +
					" if you want to use this function.");
		}
		if (redisOptions.pubsubPatterns().length == 0) {
			throw new IllegalArgumentException("" +
					"You must make sure your redis data type is " +
					PUBSUB +
					", you need to set pubsub patterns.");
		}
		client = redisOptions.createClient(getRuntimeContext());
		running.set(true);
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		
	}
}
