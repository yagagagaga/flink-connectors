package org.apache.flink.connector.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.options.RedisDataType;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.sink.RedisTableSink;
import org.apache.flink.connector.redis.source.RedisPubSubTableSource;
import org.apache.flink.connector.redis.source.RedisTableSource;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_HOST;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_HOSTS;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_MASTER;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_PORT;
import static org.apache.flink.connector.redis.options.JedisLookupOptions.REDIS_LOOKUP_ASYNC;
import static org.apache.flink.connector.redis.options.JedisLookupOptions.REDIS_LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.redis.options.JedisLookupOptions.REDIS_LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.redis.options.JedisOpOptions.REDIS_DATA_TYPE;
import static org.apache.flink.connector.redis.options.JedisOpOptions.REDIS_IGNORE_ERROR;
import static org.apache.flink.connector.redis.options.JedisOpOptions.REDIS_KEY_PATTERN;
import static org.apache.flink.connector.redis.options.JedisOpOptions.REDIS_PUBSUB_PUBLISH_CHANNEL;
import static org.apache.flink.connector.redis.options.JedisOpOptions.REDIS_PUBSUB_SUBSCRIBE_PATTERNS;
import static org.apache.flink.connector.redis.options.RedisDataType.PUBSUB;
import static org.apache.flink.connector.redis.options.RedisOptions.REDIS_DEPLOY_MODE;
import static org.apache.flink.connector.redis.options.JedisClientOptions.MAX_IDLE;
import static org.apache.flink.connector.redis.options.JedisClientOptions.MAX_TOTAL;
import static org.apache.flink.connector.redis.options.JedisClientOptions.MAX_WAIT_MILLIS;
import static org.apache.flink.connector.redis.options.JedisClientOptions.MIN_EVICTABLE_IDLE_TIME_MILLIS;
import static org.apache.flink.connector.redis.options.JedisClientOptions.MIN_IDLE;
import static org.apache.flink.connector.redis.options.JedisClientOptions.NUM_TESTS_PER_EVICTION_RUN;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_CLIENT_TIMEOUT;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_MASTERS;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_PASSWORD;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_SENTINEL;
import static org.apache.flink.connector.redis.options.JedisClientOptions.REDIS_USER;
import static org.apache.flink.connector.redis.options.JedisClientOptions.TEST_ON_BORROW;
import static org.apache.flink.connector.redis.options.JedisClientOptions.TEST_ON_RETURN;
import static org.apache.flink.connector.redis.options.JedisClientOptions.TEST_WHILE_IDLE;
import static org.apache.flink.connector.redis.options.JedisClientOptions.TIME_BETWEEN_EVICTION_RUNS_MILLIS;

/**
 * RedisTableFactory for Redis multi sentinel cluster.
 */
public class RedisTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	public static final String IDENTIFIER = "sharded-jedis";

	public static void validateTableSourceSinkOptions(ReadableConfig tableOptions, DataType physicalDataType) {
		final RedisDataType dataType = tableOptions.get(REDIS_DATA_TYPE);

		BiConsumer<List<LogicalType>, Integer> fieldTypeChecker = (children, targetFiledNum) -> {
			if (children.size() != targetFiledNum) {
				throw new ValidationException(""
						+ "If you set redis dataType="
						+ dataType
						+ ", you must make sure your table only have " + targetFiledNum + " fields.");
			}
			for (LogicalType child : children) {
				if (!children.get(0).equals(child)) {
					throw new ValidationException(""
							+ "If you set redis dataType="
							+ dataType
							+ ", you must make sure all your key and value(or elements of value) are the same type.");
				}
			}
			for (LogicalType logicalType : children) {
				if (!(logicalType instanceof VarCharType)
						&& !(logicalType instanceof BinaryType)
						&& !(logicalType instanceof VarBinaryType)) {
					throw new ValidationException("" +
							"If you set redis dataType="
							+ dataType
							+ ", you must make sure your fields type is one of "
							+ "STRING/VARCHAR/BINARY/VARBINARY/BYTES.");
				}
			}
		};

		final List<LogicalType> children = physicalDataType.getChildren()
				.stream()
				.map(DataType::getLogicalType)
				.collect(Collectors.toList());
		switch (dataType) {
			case STRING: {
				fieldTypeChecker.accept(children, 2);
				break;
			}
			case HASH: {
				switch (children.size()) {
					case 2:
						if (!(children.get(1) instanceof MapType)) {
							throw new ValidationException("" +
									"If you set redis dataType="
									+ dataType
									+ " and if your table has 2 field, you must make sure your 2nd field type is MAP.");
						}
						final MapType mapType = (MapType) children.get(1);
						final LogicalType keyType = children.get(0);
						final LogicalType fieldType = mapType.getKeyType();
						final LogicalType valueType = mapType.getValueType();
						fieldTypeChecker.accept(Arrays.asList(keyType, fieldType, valueType), 3);
						break;
					case 3:
						fieldTypeChecker.accept(children, 3);
						break;
					default:
						throw new ValidationException("" +
								"If you set redis dataType="
								+ dataType
								+ ", you must make sure your table only have 2 or 3 fields.");
				}
				break;
			}
			case LIST:
			case SET: {
				if (children.size() != 2) {
					throw new ValidationException("" +
							"If you set redis dataType="
							+ dataType
							+ ", you must make sure you have 2 field.");
				}
				if (children.get(1) instanceof ArrayType) {
					final LogicalType elementType = ((ArrayType) children.get(1)).getElementType();
					fieldTypeChecker.accept(Arrays.asList(children.get(0), elementType), 2);
				} else {
					fieldTypeChecker.accept(Arrays.asList(children.get(0), children.get(1)), 2);
				}
				break;
			}
			case SORTED_SET: {
				switch (children.size()) {
					case 2:
						if (!(children.get(1) instanceof ArrayType)) {
							throw new ValidationException("" +
									"If you set redis dataType="
									+ dataType
									+ " and if your table has 2 field, "
									+ "you must make sure your 2nd field type is ARRAY<ROW>.");
						}
						final ArrayType arrayType = (ArrayType) children.get(1);
						final LogicalType elementType = arrayType.getElementType();
						final List<LogicalType> elemChildren = elementType.getChildren();
						if (elemChildren.size() != 2 || !(elemChildren.get(0) instanceof DoubleType)) {
							throw new ValidationException("" +
									"If you set redis dataType="
									+ dataType
									+ " and if your table has 2 field, "
									+ "you must make sure your 2nd field type is ARRAY<ROW>. And the type of ROW has "
									+ "2 fields, and 1st field of ROW must be DOUBLE"
									+ "(which indicate score in redis sorted set).");
						}
						final LogicalType keyType = children.get(0);
						fieldTypeChecker.accept(Arrays.asList(keyType, elemChildren.get(1)), 2);
						break;
					case 3:
						if (!(children.get(1) instanceof DoubleType)) {
							throw new ValidationException("" +
									"If you set redis dataType="
									+ dataType
									+ " and if your table has 3 field, "
									+ "you must make sure your 2nd field type is DOUBLE"
									+ "(which indicate score in redis sorted set).");
						}
						fieldTypeChecker.accept(Arrays.asList(children.get(0), children.get(2)), 2);
						break;
					default:
						throw new ValidationException("" +
								"If you set redis dataType="
								+ dataType
								+ ", you must make sure your table only have 2 or 3 fields.");
				}
				break;
			}
			case PUBSUB: {
				final String format = tableOptions.get(FactoryUtil.FORMAT);
				if (StringUtils.isEmpty(format)) {
					throw new ValidationException("" +
							"If you set redis dataType=" + 
							dataType + 
							", you must make sure your have set `" + FactoryUtil.FORMAT.key() + "`.");
				}
				final Optional<String> optional1 = tableOptions.getOptional(REDIS_PUBSUB_SUBSCRIBE_PATTERNS);
				final Optional<String> optional2 = tableOptions.getOptional(REDIS_PUBSUB_PUBLISH_CHANNEL);
				if (!optional1.isPresent() && !optional2.isPresent()) {
					throw new ValidationException("" +
							"If you set redis dataType=" +
							dataType +
							", you need to set `" + REDIS_PUBSUB_SUBSCRIBE_PATTERNS.key() + 
							"` or `" + REDIS_PUBSUB_PUBLISH_CHANNEL.key() +"`.");
				}
				break;
			}
			default:
				throw new ValidationException(dataType + " doesn't support at this moment.");
		}
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();

		final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		final ReadableConfig tableOptions = helper.getOptions();
		validateTableSourceSinkOptions(tableOptions, physicalDataType);

		final RedisOptions redisOptions = new RedisOptions(tableOptions);
		if (redisOptions.dataType() == PUBSUB) {
			final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
					DeserializationFormatFactory.class,
					FactoryUtil.FORMAT);
			return new RedisPubSubTableSource(physicalDataType, decodingFormat, redisOptions);
		} else {
			return new RedisTableSource(physicalDataType, redisOptions);
		}
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		helper.validate();

		final DataType physicalDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		final ReadableConfig tableOptions = helper.getOptions();
		validateTableSourceSinkOptions(tableOptions, physicalDataType);

		final RedisOptions redisOptions = new RedisOptions(tableOptions);
		EncodingFormat<SerializationSchema<RowData>> encodingFormat = redisOptions.dataType() == PUBSUB
				? helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT)
				: null;
				
		return new RedisTableSink(physicalDataType, encodingFormat, redisOptions);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		final HashSet<ConfigOption<?>> options = new HashSet<>();
		options.add(REDIS_DATA_TYPE);
		options.add(REDIS_DEPLOY_MODE);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		final Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FactoryUtil.FORMAT);

		options.add(REDIS_HOST);
		options.add(REDIS_PORT);
		options.add(REDIS_HOSTS);
		options.add(REDIS_MASTER);
		options.add(REDIS_MASTERS);
		options.add(REDIS_SENTINEL);
		
		options.add(REDIS_KEY_PATTERN);
		options.add(REDIS_PUBSUB_SUBSCRIBE_PATTERNS);
		options.add(REDIS_PUBSUB_PUBLISH_CHANNEL);
		options.add(REDIS_IGNORE_ERROR);

		options.add(REDIS_USER);
		options.add(REDIS_PASSWORD);
		options.add(REDIS_CLIENT_TIMEOUT);
		options.add(MAX_WAIT_MILLIS);
		options.add(TEST_WHILE_IDLE);
		options.add(TIME_BETWEEN_EVICTION_RUNS_MILLIS);
		options.add(NUM_TESTS_PER_EVICTION_RUN);
		options.add(MIN_EVICTABLE_IDLE_TIME_MILLIS);
		options.add(MAX_TOTAL);
		options.add(MAX_IDLE);
		options.add(MIN_IDLE);
		options.add(TEST_ON_BORROW);
		options.add(TEST_ON_RETURN);
		
		options.add(REDIS_LOOKUP_CACHE_MAX_ROWS);
		options.add(REDIS_LOOKUP_CACHE_TTL);
		options.add(REDIS_LOOKUP_ASYNC);
		return options;
	}
}
