package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.connector.redis.client.RedisClientProxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@FunctionalInterface
public interface RedisRecordProducer<T> extends Function {
	
	Logger LOG = LoggerFactory.getLogger(RedisRecordProducer.class);
	
	List<T> apply(List<byte[]> keys, RedisClientProxy client, boolean ignoreError);
}
