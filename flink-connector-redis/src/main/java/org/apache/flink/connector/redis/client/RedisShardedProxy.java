package org.apache.flink.connector.redis.client;

import org.apache.flink.util.IOUtils;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.Tuple;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisShardedProxy implements RedisClientProxy {

	private final ShardedJedis shardedJedis;
	private final JedisSentinelPools jedisSentinelPools;
	
	private ShardedJedis jedisPipelined;

	public RedisShardedProxy(JedisSentinelPools jedisSentinelPools) {
		this.jedisSentinelPools = jedisSentinelPools;
		ShardedJedisPool
		this.shardedJedis = new ShardedJedisSentinel(jedisSentinelPools.getShards());
	}

	protected ShardedJedis getInstance() {
		return new ShardedJedisSentinel(jedisSentinelPools.getShards());
	}

	@Override
	public void close() {
		IOUtils.closeQuietly(shardedJedis);
		IOUtils.closeQuietly(jedisPipelined);
		IOUtils.closeQuietly(jedisSentinelPools);
	}
	
	@Override
	public byte[] hget(byte[] key, byte[] field) {
		try (ShardedJedis j = getInstance()) {
			return j.hget(key, field);
		}
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		try (ShardedJedis j = getInstance()) {
			return j.hgetAll(key);
		}
	}

	@Override
	public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
		try (ShardedJedis j = getInstance()) {
			return j.hscan(key, cursor, params);
		}
	}

	@Override
	public List<byte[]> lrange(byte[] key, long start, long stop) {
		try (ShardedJedis j = getInstance()) {
			return j.lrange(key, start, stop);
		}
	}

	@Override
	public Set<byte[]> smembers(byte[] key) {
		try (ShardedJedis j = getInstance()) {
			return j.smembers(key);
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		try (ShardedJedis j = getInstance()) {
			return j.zrangeByScoreWithScores(key, min, max);
		}
	}

	@Override
	public Set<Tuple> zrangeWithScores(byte[] key, long start, long stop) {
		try (ShardedJedis j = getInstance()) {
			return j.zrangeWithScores(key, start, stop);
		}
	}

	@Override
	public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
		try (ShardedJedis j = getInstance()) {
			return j.zscan(key, cursor, params);
		}
	}

	@Override
	public byte[] get(byte[] key) {
		try (ShardedJedis j = getInstance()) {
			return j.get(key);
		}
	}

	@Override
	public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
		throw new IllegalStateException("Not support now!");
	}

	@Override
	public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
		throw new IllegalStateException("Not support now!");
	}

	@Override
	public synchronized PipelineBase pipelined() {
		if (jedisPipelined == null) {
			jedisPipelined = getInstance();
			return jedisPipelined.pipelined();
		} else {
			org.apache.commons.io.IOUtils.closeQuietly(jedisPipelined);
			jedisPipelined = null;
			return pipelined();
		}
	}

	public Collection<Jedis> getAllShards() {
		return shardedJedis.getAllShards();
	}
}
