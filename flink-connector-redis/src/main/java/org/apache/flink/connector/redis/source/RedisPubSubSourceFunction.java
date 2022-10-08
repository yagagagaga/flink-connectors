package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * RedisPubSubSourceFunction for standard input stream.
 */
public class RedisPubSubSourceFunction<T> extends RichSourceFunction<T> {

	private final RedisPubSubParallelSourceFunction<T> delegate;

	public RedisPubSubSourceFunction(RedisOptions redisOptions, DeserializationSchema<T> deserializationSchema) {
		delegate = new RedisPubSubParallelSourceFunction<>(redisOptions, deserializationSchema);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		delegate.setRuntimeContext(getRuntimeContext());
		delegate.open(parameters);
	}

	@Override
	public void run(SourceContext<T> sourceContext) {
		delegate.run(sourceContext);
	}

	@Override
	public void cancel() {
		delegate.cancel();
	}
}
