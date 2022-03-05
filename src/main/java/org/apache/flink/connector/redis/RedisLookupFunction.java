package org.apache.flink.connector.redis;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class RedisLookupFunction extends TableFunction<Row> {

    private transient Jedis jedis;
    private final RedisOptions redisOptions;
    private final TypeInformation<Row> resultType;
    private final DeserializationSchema<Row> deserializationSchema;

    public RedisLookupFunction(RedisOptions redisOptions, TypeInformation<Row> resultType, DeserializationSchema<Row> deserializationSchema) {
        this.redisOptions = redisOptions;
        this.resultType = resultType;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        jedis = new Jedis(redisOptions.host, redisOptions.port);
        jedis.ping();
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return resultType;
    }

    @SuppressWarnings({"unused", "RedundantSuppression"})
    public void eval(Object rowKey) throws IOException {
        if (rowKey == null) {
            return;
        }
        byte[] value;
        if (rowKey instanceof byte[]) {
            value = jedis.get(((byte[]) rowKey));
        } else {
            value = jedis.get(rowKey.toString().getBytes());
        }
        collect(deserializationSchema.deserialize(value));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
