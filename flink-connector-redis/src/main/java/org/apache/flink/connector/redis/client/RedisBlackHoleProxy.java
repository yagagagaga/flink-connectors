package org.apache.flink.connector.redis.client;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis command container if we want to connect to a Redis cluster.
 */
class RedisBlackHoleProxy implements RedisClientProxy {

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		return new byte[0];
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		return null;
	}

	@Override
	public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
		return null;
	}

	@Override
	public List<byte[]> lrange(byte[] key, long start, long stop) {
		return null;
	}

	@Override
	public Set<byte[]> smembers(byte[] key) {
		return null;
	}

	@Override
	public Set<Tuple> zrangeWithScores(byte[] key, long start, long stop) {
		return null;
	}

	@Override
	public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
		return null;
	}

	@Override
	public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
		return null;
	}

	@Override
	public byte[] get(byte[] key) {
		return new byte[0];
	}

	@Override
	public PipelineBase pipelined() {
		return null;
	}

	@Override
	public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
		return null;
	}

	@Override
	public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {

	}

	@Override
	public void close() {

	}
}
