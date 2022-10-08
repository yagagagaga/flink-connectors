package org.apache.flink.connector.redis.client;

import org.apache.commons.io.IOUtils;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisProxy implements RedisClientProxy, Closeable {

    private static final long serialVersionUID = 1L;

    private final Jedis j;

    public RedisProxy(Jedis j) {
        this.j = j;
    }

    /**
     * Closes the Jedis instances.
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(j);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return j.hget(key, field);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return j.hgetAll(key);
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(byte[] key, byte[] cursor, ScanParams params) {
        return j.hscan(key, cursor, params);
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long stop) {
        return j.lrange(key, start, stop);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        return j.smembers(key);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long stop) {
        return j.zrangeWithScores(key, start, stop);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return j.zrangeByScoreWithScores(key, min, max);
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] key, byte[] cursor, ScanParams params) {
        return j.zscan(key, cursor, params);
    }

    @Override
    public byte[] get(byte[] key) {
        return j.get(key);
    }

    @Override
    public ScanResult<byte[]> scan(byte[] cursor, ScanParams params) {
        return j.scan(cursor, params);
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        j.psubscribe(jedisPubSub, patterns);
    }

    @Override
    public PipelineBase pipelined() {
        return j.pipelined();
    }
}
