package org.apache.flink.connector.redis;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.RedisValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.DescriptorProperties.TABLE_SCHEMA_EXPR;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_CHANNEL_PATTERN;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_HOST;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_KEY_POS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PORT;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PROPERTIES;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PROPERTIES_KEY;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PROPERTIES_VALUE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_TYPE_VALUE_REDIS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_FROM;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_TIMESTAMPS_TYPE;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_CLASS;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_DELAY;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_SERIALIZED;
import static org.apache.flink.table.descriptors.Rowtime.ROWTIME_WATERMARKS_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_PROCTIME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;

public class RedisTableFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Tuple2<Boolean, Row>> {
	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final RedisOptions redisOptions = RedisOptions.getRedisOptions(descriptorProperties);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));

		return new RedisRetractTableSink(redisOptions, tableSchema, getSerializationSchema(properties));
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);

		final RedisOptions redisOptions = RedisOptions.getRedisOptions(descriptorProperties);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(descriptorProperties.getTableSchema(SCHEMA));

		return new RedisTableSource(redisOptions, tableSchema, getDeserializationSchema(properties));
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new RedisValidator().validate(descriptorProperties);
		return descriptorProperties;
	}

	private SerializationSchema<Row> getSerializationSchema(Map<String, String> properties) {
		@SuppressWarnings("unchecked")
		final SerializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
				SerializationSchemaFactory.class, properties, this.getClass().getClassLoader());
		return formatFactory.createSerializationSchema(properties);
	}

	private DeserializationSchema<Row> getDeserializationSchema(Map<String, String> properties) {
		@SuppressWarnings("unchecked")
		final DeserializationSchemaFactory<Row> formatFactory = TableFactoryService.find(
				DeserializationSchemaFactory.class,
				properties,
				this.getClass().getClassLoader());
		return formatFactory.createDeserializationSchema(properties);
	}

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_REDIS);
		context.put(CONNECTOR_VERSION, "universal");
		context.put(CONNECTOR_PROPERTY_VERSION, "1");
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		// update mode
		properties.add(UPDATE_MODE);

		// redis
		properties.add(CONNECTOR_HOST);
		properties.add(CONNECTOR_PORT);
		properties.add(CONNECTOR_DATA_STRUCTURE);
		properties.add(CONNECTOR_KEY_POS);
		properties.add(CONNECTOR_CHANNEL_PATTERN);
		properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_KEY);
		properties.add(CONNECTOR_PROPERTIES + ".#." + CONNECTOR_PROPERTIES_VALUE);
		properties.add(CONNECTOR_PROPERTIES + ".*");

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + SCHEMA_FROM);

		properties.add(SCHEMA + ".#." + TABLE_SCHEMA_EXPR);

		properties.add(SCHEMA + ".#." + SCHEMA_PROCTIME);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_FROM);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_TIMESTAMPS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_TYPE);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_CLASS);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_SERIALIZED);
		properties.add(SCHEMA + ".#." + ROWTIME_WATERMARKS_DELAY);

		// watermark
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);

		// format wildcard
		properties.add(FORMAT + ".*");
		return properties;
	}
}
