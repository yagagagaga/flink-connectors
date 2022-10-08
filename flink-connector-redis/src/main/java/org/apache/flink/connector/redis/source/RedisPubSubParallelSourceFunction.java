package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import org.apache.commons.io.IOUtils;
import redis.clients.jedis.BinaryJedisPubSub;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.connector.redis.options.RedisDataType.PUBSUB;

/**
 * RedisPubSubParallelSourceFunction for standard input stream.
 */
public class RedisPubSubParallelSourceFunction<T> extends RichParallelSourceFunction<T> {

	private final RedisOptions redisOptions;
	private final DeserializationSchema<T> deserializationSchema;
	private final AtomicBoolean running = new AtomicBoolean(false);

	private RedisClientProxy client;
	private BinaryJedisPubSub pubSub;

	public RedisPubSubParallelSourceFunction(RedisOptions redisOptions, DeserializationSchema<T> deserializationSchema) {
		this.redisOptions = redisOptions;
		this.deserializationSchema = deserializationSchema;
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
	public void run(SourceContext<T> sourceContext) {
		if (pubSub == null) {
			pubSub = new MyPubSub<>(running, sourceContext, deserializationSchema);
		}
		client.psubscribe(pubSub, redisOptions.pubsubPatterns());
	}

	@Override
	public void cancel() {
		running.set(false);
		pubSub.punsubscribe(redisOptions.pubsubPatterns());
		IOUtils.closeQuietly(client);
	}

	/**
	 * A JedisPubSub implementation.
	 */
	public static class MyPubSub<T> extends BinaryJedisPubSub {

		private final AtomicBoolean running;
		private final SourceContext<T> sourceContext;
		private final DeserializationSchema<T> deserializationSchema;

		public MyPubSub(
				AtomicBoolean running,
				SourceContext<T> sourceContext,
				DeserializationSchema<T> deserializationSchema) {
			this.running = running;
			this.sourceContext = sourceContext;
			this.deserializationSchema = deserializationSchema;
		}

		@Override
		public boolean isSubscribed() {
			return running.get() && super.isSubscribed();
		}

		@Override
		public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
			try {
				final T t = deserializationSchema.deserialize(message);
				sourceContext.collect(t);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
