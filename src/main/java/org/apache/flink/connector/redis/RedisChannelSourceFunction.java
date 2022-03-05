package org.apache.flink.connector.redis;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE_VALUE_SUBSCRIBE;

public class RedisChannelSourceFunction<T> extends RichSourceFunction<T> {

	private final RedisOptions redisOptions;
	private final DeserializationSchema<T> deserializationSchema;
	private boolean running = false;
	private Jedis jedis;
	private JedisPubSub pubSub;

	public RedisChannelSourceFunction(RedisOptions redisOptions, DeserializationSchema<T> deserializationSchema) {
		this.redisOptions = redisOptions;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (!redisOptions.dataStructure.equals(CONNECTOR_DATA_STRUCTURE_VALUE_SUBSCRIBE)) {
			throw new RuntimeException("不支持除了【" + CONNECTOR_DATA_STRUCTURE_VALUE_SUBSCRIBE + "】之外的其他数据结构");
		}
		jedis = new Jedis(redisOptions.host, redisOptions.port);
		jedis.ping();
		running = true;
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		if (pubSub == null) {
			pubSub = new MyPubSub<>(sourceContext, deserializationSchema);
		}
		
		while (running) {
			jedis.psubscribe(pubSub, redisOptions.channelPattern);
		}
	}

	@Override
	public void cancel() {
		running = false;
		pubSub.punsubscribe(redisOptions.channelPattern);
		jedis.close();
	}

	public static class MyPubSub<T> extends JedisPubSub {

		private final SourceContext<T> sourceContext;
		private final DeserializationSchema<T> deserializationSchema;

		public MyPubSub(SourceContext<T> sourceContext, DeserializationSchema<T> deserializationSchema) {
			this.sourceContext = sourceContext;
			this.deserializationSchema = deserializationSchema;
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
			super.onPMessage(pattern, channel, message);
			try {
				final T t = deserializationSchema.deserialize(message.getBytes());
				sourceContext.collect(t);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
