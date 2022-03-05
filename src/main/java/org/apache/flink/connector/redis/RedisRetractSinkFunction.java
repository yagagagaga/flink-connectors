package org.apache.flink.connector.redis;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE_VALUE_HASH;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE_VALUE_LIST;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE_VALUE_PUBLISH;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE_VALUE_SET;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE_VALUE_STRING;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE_VALUE_ZSET;

public class RedisRetractSinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {

    private final RedisOptions redisOptions;
    private final SerializationSchema<Row> serializationSchema;

    private Jedis jedis;
    private Pipeline pipeline;
    private int batchCount = 0;

    private Counter recordsCounter;
    private WatermarkGauge startTime;
    private WatermarkGauge consumeTime;

    public RedisRetractSinkFunction(RedisOptions redisOptions, SerializationSchema<Row> serializationSchema) {
        this.redisOptions = redisOptions;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jedis = new Jedis(redisOptions.host, redisOptions.port);
        jedis.ping();
        pipeline = jedis.pipelined();

        recordsCounter = new SimpleCounter();
        startTime = new WatermarkGauge();
        startTime.setCurrentWatermark(System.currentTimeMillis());
        consumeTime = new WatermarkGauge();

        getRuntimeContext().getMetricGroup()
                .addGroup("flink-redis")
                .counter("redis_record_counter", recordsCounter);
        getRuntimeContext().getMetricGroup()
                .addGroup("flink-redis")
                .gauge("redis_start_time", startTime);
        getRuntimeContext().getMetricGroup()
                .addGroup("flink-redis")
                .gauge("redis_consume_time", consumeTime);
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> record, Context context) {
        recordsCounter.inc();

        boolean needRetract = !record.f0;
        Row row = record.f1;
        if (needRetract) {
            del(row);
        } else {
            put(row);
        }
        batchCount++;
        if (batchCount >= redisOptions.maxFlushSize) {
            pipeline.sync();
            batchCount = 0;
        }

        consumeTime.setCurrentWatermark(System.currentTimeMillis() - startTime.getValue());
    }

    @Override
    public void close() throws Exception {
        super.close();
        pipeline.close();
        jedis.close();
    }

    private void put(Row row) {
        switch (redisOptions.dataStructure) {
            case CONNECTOR_DATA_STRUCTURE_VALUE_STRING:
                setString(row);
                break;
            case CONNECTOR_DATA_STRUCTURE_VALUE_LIST:
                setList(row);
                break;
            case CONNECTOR_DATA_STRUCTURE_VALUE_HASH:
                setHash(row);
                break;
            case CONNECTOR_DATA_STRUCTURE_VALUE_SET:
                setSet(row);
                break;
            case CONNECTOR_DATA_STRUCTURE_VALUE_ZSET:
                setZSet(row);
                break;
            case CONNECTOR_DATA_STRUCTURE_VALUE_PUBLISH:
                publish(row);
                break;
            default:
                throw new IllegalStateException("" +
                        "目前不支持redis的【" + redisOptions.dataStructure + "】这种数据结构，仅支持以下几种：\n" +
                        "string\nlist\nhash\nset\nzset");
        }
    }

    private void del(Row row) {
        final byte[] key = getKey(row);
        if (key != null) {
            pipeline.del(key);
        }
    }

    private byte[] getKey(Row row) {
        final Object field = row.getField(redisOptions.keyPos);
        if (field == null) {
            return null;
        } else if (field instanceof byte[]) {
            return ((byte[]) field);
        } else {
            return field.toString().getBytes();
        }
    }

    private byte[] getValue(Row row) {
        return serializationSchema.serialize(row);
    }

    private void setString(Row row) {
        final byte[] key = getKey(row);
        if (key == null) {
            return;
        }
        final byte[] value = getValue(row);
        pipeline.set(key, value);
    }

    private void setList(Row row) {
        final byte[] key = getKey(row);
        if (key == null) {
            return;
        }
        final byte[] value = getValue(row);
        pipeline.lpush(key, value);
    }

    private void setHash(Row row) {
        String key = String.valueOf(row.getField(0));
        String field = String.valueOf(row.getField(1));
        Object value = row.getField(2);
        if (value instanceof byte[]) {
            pipeline.hset(key.getBytes(), field.getBytes(), ((byte[]) value));
        } else {
            pipeline.hset(key, field, String.valueOf(value));
        }
    }

    private void setSet(Row row) {
        final byte[] key = getKey(row);
        if (key == null) {
            return;
        }
        final byte[] value = getValue(row);
        pipeline.sadd(key, value);
    }

    private void setZSet(Row row) {
        final byte[] key = getKey(row);
        if (key == null) {
            return;
        }
        final byte[] value = getValue(row);
        pipeline.zadd(key, 0, value);
    }

    private void publish(Row row) {
        final byte[] value = serializationSchema.serialize(row);
        pipeline.publish(redisOptions.channelPattern.getBytes(), value);
    }
}
