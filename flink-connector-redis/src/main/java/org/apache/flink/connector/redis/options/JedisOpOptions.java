package org.apache.flink.connector.redis.options;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.sink.RedisHashConsumer;
import org.apache.flink.connector.redis.sink.RedisListConsumer;
import org.apache.flink.connector.redis.sink.RedisPublicConsumer;
import org.apache.flink.connector.redis.sink.RedisRecordConsumer;
import org.apache.flink.connector.redis.sink.RedisSetConsumer;
import org.apache.flink.connector.redis.sink.RedisSortedSetConsumer;
import org.apache.flink.connector.redis.sink.RedisStringConsumer;
import org.apache.flink.connector.redis.source.RedisHashProducer;
import org.apache.flink.connector.redis.source.RedisListProducer;
import org.apache.flink.connector.redis.source.RedisRecordProducer;
import org.apache.flink.connector.redis.source.RedisSetProducer;
import org.apache.flink.connector.redis.source.RedisSortedSetProducer;
import org.apache.flink.connector.redis.source.RedisStringProducer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.redis.options.RedisDataType.PUBSUB;

public final class JedisOpOptions implements Serializable {

	public static final ConfigOption<RedisDataType> REDIS_DATA_TYPE = ConfigOptions
			.key("redis.data-type")
			.enumType(RedisDataType.class)
			.noDefaultValue()
			.withDescription("Data type for Redis.");

	public static final ConfigOption<String> REDIS_KEY_PATTERN = ConfigOptions
			.key("redis.key-pattern")
			.stringType()
			.noDefaultValue()
			.withDescription("Scan key pattern for Redis.");
	
	public static final ConfigOption<String> REDIS_PUBSUB_SUBSCRIBE_PATTERNS = ConfigOptions
			.key("redis.pubsub.subscribe-patterns")
			.stringType()
			.noDefaultValue()
			.withDescription("Subscribe patterns for Redis PUBSUB.");
	
	public static final ConfigOption<String> REDIS_PUBSUB_PUBLISH_CHANNEL = ConfigOptions
			.key("redis.pubsub.publish-channel")
			.stringType()
			.noDefaultValue()
			.withDescription("Publish channel for Redis PUBSUB.");
	
	public static final ConfigOption<Boolean> REDIS_IGNORE_ERROR = ConfigOptions
			.key("redis.ignore-error")
			.booleanType()
			.defaultValue(false)
			.withDescription("Ignore error when redis query/insert.");
	
	public static final ConfigOption<Integer> REDIS_BATCH_SIZE = ConfigOptions
			.key("redis.batch-size")
			.intType()
			.defaultValue(1)
			.withDescription("Batch of Redis sink.");
	
	public static final ConfigOption<Long> REDIS_LINGER_MS = ConfigOptions
			.key("redis.linger-ms")
			.longType()
			.defaultValue(200L)
			.withDescription("Max delay to send a Batch of Redis sink.");

	private static final long serialVersionUID = 1L;

	private final RedisDataType dataType;
	@Nullable
	private final String keyPattern;
	private final boolean ignoreError;
	private final int batchSize;
	private final long lingerMs;
	private final byte[][] pubsubPatterns;
	private final String channel;

	public JedisOpOptions(ReadableConfig tableOptions) {
		this.dataType = tableOptions.get(REDIS_DATA_TYPE);
		this.keyPattern = tableOptions.getOptional(REDIS_KEY_PATTERN).orElse(null);
		this.ignoreError = tableOptions.get(REDIS_IGNORE_ERROR);
		this.batchSize = tableOptions.get(REDIS_BATCH_SIZE);
		this.lingerMs = tableOptions.get(REDIS_LINGER_MS);
		this.pubsubPatterns = Arrays
				.stream(tableOptions.getOptional(REDIS_PUBSUB_SUBSCRIBE_PATTERNS).orElse("").split(","))
				.map(String::getBytes)
				.toArray(byte[][]::new);
		this.channel = tableOptions.getOptional(REDIS_PUBSUB_PUBLISH_CHANNEL).orElse("");
	}

	@Nonnull
	public String keyPattern() {
		if (keyPattern != null) {
			return keyPattern;
		} else {
			throw new NullPointerException(REDIS_KEY_PATTERN + " shouldn't be null.");
		}
	}

	@Nonnull
	public byte[][] pubsubPatterns() {
		return pubsubPatterns;
	}

	@Nonnull
	public String channel() {
		return channel;
	}

	public RedisRecordConsumer<RowData> createRecordConsumer(DataType physicalDataType) {
		final List<LogicalType> logicalTypes = physicalDataType.getChildren()
				.stream()
				.map(DataType::getLogicalType)
				.collect(Collectors.toList());
		
		switch (dataType) {
			case STRING:
				return new RedisStringConsumer(logicalTypes.get(0) instanceof VarCharType);
			case HASH:
				return new RedisHashConsumer(
						logicalTypes.get(0) instanceof VarCharType,
						logicalTypes.get(1) instanceof MapType);
			case LIST:
				return new RedisListConsumer(logicalTypes.get(0) instanceof VarCharType);
			case SET:
				return new RedisSetConsumer(
						logicalTypes.get(0) instanceof VarCharType,
						logicalTypes.get(1) instanceof ArrayType);
			case SORTED_SET:
				return new RedisSortedSetConsumer(
						logicalTypes.get(0) instanceof VarCharType,
						logicalTypes.get(1) instanceof ArrayType);
			default:
				throw new IllegalArgumentException("Not support " + dataType + " now!");
		}
	}

	public RedisRecordConsumer<RowData> createRecordConsumer(
			String channel, @Nullable SerializationSchema<RowData> serializationSchema) {
		if (dataType != PUBSUB) {
			throw new IllegalArgumentException("Not support " + dataType + " now!");
		}
		if (serializationSchema == null) {
			throw new IllegalArgumentException("" +
					"Require `" +
					FactoryUtil.FORMAT + 
					"` if you set dataType = pubsub.");
		}
		if (StringUtils.isEmpty(channel)) {
			throw new IllegalArgumentException("" +
					"Require `" +
					REDIS_PUBSUB_PUBLISH_CHANNEL + 
					"` if you set dataType = pubsub.");
		}
		return new RedisPublicConsumer(channel, serializationSchema);
	}

	public RedisRecordProducer<RowData> createRecordProducer(DataType physicalDataType) {
		final List<LogicalType> logicalTypes = physicalDataType.getChildren()
				.stream()
				.map(DataType::getLogicalType)
				.collect(Collectors.toList());
		
		switch (dataType) {
			case STRING:
				return new RedisStringProducer(logicalTypes.get(0) instanceof VarCharType);
			case HASH:
				return new RedisHashProducer(
						logicalTypes.get(0) instanceof VarCharType,
						logicalTypes.get(1) instanceof MapType
				);
			case LIST:
				return new RedisListProducer(logicalTypes.get(0) instanceof VarCharType);
			case SET:
				return new RedisSetProducer(logicalTypes.get(0) instanceof VarCharType);
			case SORTED_SET:
				return new RedisSortedSetProducer(
						logicalTypes.get(0) instanceof VarCharType,
						logicalTypes.get(1) instanceof ArrayType);
			default:
				throw new IllegalArgumentException("Not support " + dataType + " now!");
		}
	}

	public RedisDataType dataType() {
		return dataType;
	}

	public boolean ignoreError() {
		return ignoreError;
	}
	
	public int batchSize() {
		return batchSize;
	}
	
	public long lingerMs() {
		return lingerMs;
	}
}
