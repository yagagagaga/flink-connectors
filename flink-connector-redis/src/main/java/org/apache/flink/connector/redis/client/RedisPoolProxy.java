package org.apache.flink.connector.redis.client;

import org.apache.commons.io.IOUtils;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisPoolProxy implements RedisClientProxy {

	private static final long serialVersionUID = 1L;

	private final JedisSentinelPool sentinelPool;
	private final JedisPool pool;
	private Jedis jedisPipelined;

	public RedisPoolProxy(JedisSentinelPool sentinelPool) {
		this.sentinelPool = sentinelPool;
		this.pool = null;
	}

	public RedisPoolProxy(JedisPool pool) {
		this.sentinelPool = null;
		this.pool = pool;
	}

	protected Jedis getInstance() {
		if (pool != null) {
			return pool.getResource();
		} else if (sentinelPool != null) {
			return sentinelPool.getResource();
		} else {
			throw new IllegalStateException("This cannot happen!");
		}
	}

	/**
	 * Closes the Jedis instances.
	 */
	@Override
	public void close() {
		IOUtils.closeQuietly(sentinelPool);
		IOUtils.closeQuietly(pool);
	}

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		try (Jedis j = getInstance()) {
			return j.hget(key, field);
		}
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		try (Jedis j = getInstance()) {
			return j.hgetAll(key);
		}
	}

	@Override
	public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
		try (Jedis j = getInstance()) {
			return j.hscan(key, cursor, params);
		}
	}

	@Override
	public List<byte[]> lrange(byte[] key, long start, long stop) {
		try (Jedis j = getInstance()) {
			return j.lrange(key, start, stop);
		}
	}

	@Override
	public Set<byte[]> smembers(byte[] key) {
		try (Jedis j = getInstance()) {
			return j.smembers(key);
		}
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		try (Jedis j = getInstance()) {
			return j.zrangeByScoreWithScores(key, min, max);
		}
	}

	@Override
	public Set<Tuple> zrangeWithScores(byte[] key, long start, long stop) {
		try (Jedis j = getInstance()) {
			return j.zrangeWithScores(key, start, stop);
		}
	}

	@Override
	public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
		try (Jedis j = getInstance()) {
			return j.zscan(key, cursor, params);
		}
	}

	@Override
	public byte[] get(byte[] key) {
		try (Jedis j = getInstance()) {
			return j.get(key);
		}
	}

	@Override
	public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
		try (Jedis j = getInstance()) {
			return j.scan(cursor, params);
		}
	}

	@Override
	public synchronized PipelineBase pipelined() {
		if (jedisPipelined == null) {
			jedisPipelined = getInstance();
			return jedisPipelined.pipelined();
		} else {
			IOUtils.closeQuietly(jedisPipelined);
			jedisPipelined = null;
			return pipelined();
		}
	}

	@Override
	public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
		try (Jedis j = getInstance()) {
			j.psubscribe(jedisPubSub, patterns);
		}
	}
}
