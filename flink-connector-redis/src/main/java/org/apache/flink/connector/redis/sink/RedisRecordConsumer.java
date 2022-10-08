package org.apache.flink.connector.redis.sink;

import org.apache.flink.api.common.functions.Function;

import redis.clients.jedis.PipelineBase;

@FunctionalInterface
public interface RedisRecordConsumer<T> extends Function {
	void apply(T record, PipelineBase client);

	@FunctionalInterface
	interface KeySelector<I, K> extends Function {
		K apply(I in);
	}

	@FunctionalInterface
	interface ValueSelector<I, V> extends Function {
		V apply(I in);
	}
}
