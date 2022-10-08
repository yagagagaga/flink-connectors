package org.apache.flink.connector.redis.options;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.client.JedisSentinelPools;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.connector.redis.client.RedisClusterProxy;
import org.apache.flink.connector.redis.client.RedisPoolProxy;
import org.apache.flink.connector.redis.client.RedisShardedProxy;
import org.apache.flink.connector.redis.sink.RedisRecordConsumer;
import org.apache.flink.connector.redis.source.RedisRecordProducer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.Properties;

import static org.apache.flink.connector.redis.client.RedisClientProxy.BLACK_HOLE;
import static org.apache.flink.connector.redis.options.RedisDataType.PUBSUB;
import static org.apache.flink.connector.redis.options.RedisDeployMode.SHARDED;
import static org.apache.flink.connector.redis.options.RedisDeployMode.SINGLE;

/**
 * Option utils for ShardedJedis table source sink.
 */
public final class RedisOptions implements Serializable {

	public static final ConfigOption<RedisDeployMode> REDIS_DEPLOY_MODE = ConfigOptions
			.key("redis.deploy-mode")
			.enumType(RedisDeployMode.class)
			.defaultValue(SINGLE)
			.withDescription("Deploy mode for Redis.");

	private final RedisDeployMode deployMode;
	private final JedisOpOptions opOptions;
	private final JedisLookupOptions lookupOptions;
	private final JedisClientOptions redisClientOptions;

	public RedisOptions(Properties properties) {
		this(Configuration.fromMap(Maps.fromProperties(properties)));
	}

	public RedisOptions(ReadableConfig tableOptions) {
		this.deployMode = tableOptions.get(REDIS_DEPLOY_MODE);
		this.redisClientOptions = new JedisClientOptions(tableOptions);
		this.opOptions = new JedisOpOptions(tableOptions);
		this.lookupOptions = new JedisLookupOptions(tableOptions);
	}

	public RedisRecordProducer<RowData> createRecordProducer(DataType physicalDataType) {
		return opOptions.createRecordProducer(physicalDataType);
	}

	public RedisRecordConsumer<RowData> createRecordConsumer(DataType physicalDataType,
															 SerializationSchema<RowData> serializationSchema) {
		if (dataType() == PUBSUB) {
			return opOptions.createRecordConsumer(channel(), serializationSchema);
		} else {
			return opOptions.createRecordConsumer(physicalDataType);
		}
	}

	public RedisDeployMode deployMode() {
		return deployMode;
	}

	public RedisDataType dataType() {
		return opOptions.dataType();
	}

	@Nonnull
	public byte[] keyPattern() {
		return opOptions.keyPattern().getBytes();
	}

	public boolean ignoreError() {
		return opOptions.ignoreError();
	}

	public int batchSize() {
		return opOptions.batchSize();
	}

	public long lingerMs() {
		return opOptions.lingerMs();
	}

	public long cacheMaxSize() {
		return lookupOptions.cacheMaxSize();
	}

	public long cacheExpireMs() {
		return lookupOptions.cacheExpireMs();
	}

	public boolean async() {
		return lookupOptions.async();
	}

	public RedisClientProxy createClient() {
		switch (deployMode) {
			case SINGLE:
				return new RedisPoolProxy(redisClientOptions.createSingleJedisPool());
			case SENTINEL:
				return new RedisPoolProxy(redisClientOptions.createSentinelJedisPool());
			case CLUSTER:
				return new RedisClusterProxy(redisClientOptions.createJedisCluster());
			case SHARDED:
				return new RedisShardedProxy(redisClientOptions.createJedisSentinelPools());
			default:
				throw new IllegalArgumentException("Unknown deploy mode: " + deployMode);
		}
	}

	public RedisClientProxy createClient(RuntimeContext ctx) {
		if (deployMode != SHARDED) {
			return createClient();
		}
		int parallelism = ctx.getNumberOfParallelSubtasks();
		int subtaskIdx = ctx.getIndexOfThisSubtask();
		final JedisSentinelPools pools = redisClientOptions.createJedisSentinelPools(subtaskIdx, parallelism);
		return pools == null ? BLACK_HOLE : new RedisShardedProxy(pools);
	}

	@Nonnull
	public byte[][] pubsubPatterns() {
		return opOptions.pubsubPatterns();
	}

	@Nonnull
	public String channel() {
		return opOptions.channel();
	}
}
