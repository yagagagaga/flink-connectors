package org.apache.flink.connector.redis.client;

import org.apache.commons.io.IOUtils;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Redis command container if we want to connect to a Redis cluster.
 */
public class RedisClusterProxy implements RedisClientProxy {

    private static final long serialVersionUID = 1L;

    private final transient JedisCluster jedisCluster;

    /**
     * Initialize Redis command container for Redis cluster.
     *
     * @param jedisCluster JedisCluster instance
     */
    public RedisClusterProxy(JedisCluster jedisCluster) {
        Objects.requireNonNull(jedisCluster, "Jedis cluster can not be null");
        this.jedisCluster = jedisCluster;
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return jedisCluster.hget(key, field);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return jedisCluster.hgetAll(key);
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
        return jedisCluster.hscan(key, cursor, params);
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long stop) {
        return jedisCluster.lrange(key, start, stop);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return jedisCluster.smembers(key);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long stop) {
        return jedisCluster.zrangeWithScores(key, start, stop);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return jedisCluster.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
        return jedisCluster.zscan(key, cursor, params);
    }

    @Override
    public byte[] get(byte[] key) {
        return jedisCluster.get(key);
    }

    @Override
    public PipelineBase pipelined() {
        return null;
    }

    @Override
    public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
        return jedisCluster.scan(cursor, params);
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        jedisCluster.psubscribe(jedisPubSub, patterns);
    }

    /**
     * Closes the {@link JedisCluster}.
     */
    @Override
    public void close() {
        IOUtils.closeQuietly(jedisCluster);
    }
}
