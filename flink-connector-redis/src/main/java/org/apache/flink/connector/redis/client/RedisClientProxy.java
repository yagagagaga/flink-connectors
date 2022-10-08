package org.apache.flink.connector.redis.client;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The container for all available Redis commands.
 */
public interface RedisClientProxy extends Serializable, Closeable {

    RedisClientProxy BLACK_HOLE = new RedisBlackHoleProxy();
    
    byte[] hget(final byte[] key, final byte[] field);
    Map<byte[], byte[]> hgetAll(final byte[] key);
    ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor, final ScanParams params);
    
    List<byte[]> lrange(final byte[] key, final long start, final long stop);
    
    Set<byte[]> smembers(final byte[] key);
    
    Set<Tuple> zrangeWithScores(final byte[] key, final long start, final long stop);
    Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max);
    ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor, final ScanParams params);
    
    byte[] get(final byte[] key);

    PipelineBase pipelined();

    ScanResult<byte[]> scan(final byte[] cursor, final ScanParams params);

    void psubscribe(BinaryJedisPubSub jedisPubSub, final byte[]... patterns);
}
