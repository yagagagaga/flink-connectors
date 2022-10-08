package org.apache.flink.connector.redis.options;

public enum RedisDeployMode {
    CLUSTER,
    SENTINEL,
    SINGLE,
    SHARDED
}
