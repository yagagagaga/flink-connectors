package org.apache.flink.connector.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;

import java.io.Serializable;
import java.util.List;

public class ShardedJedisSentinel extends ShardedJedis implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(ShardedJedisSentinel.class);
	
	public ShardedJedisSentinel(List<JedisShardInfo> shards) {
		super(shards);
		LOG.info("ShardedJedisSentinel Connection ... ");
	}

	@Override
	public void close() {

		for (Jedis jedis : getAllShards()) {
			try {
				jedis.close();
			} catch (Exception ignored) {
				// ignore the exception node,
				// so that all other normal nodes can release all connections.
			}
		}
	}
}
