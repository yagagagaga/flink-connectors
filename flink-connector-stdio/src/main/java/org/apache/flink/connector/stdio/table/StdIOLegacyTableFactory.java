package org.apache.flink.connector.stdio.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.stdio.sink.StdIOLegacyTableSink;
import org.apache.flink.connector.stdio.source.StdIOLegacyTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.StdIOValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
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

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.PRIMARY_KEY_COLUMNS;
import static org.apache.flink.table.descriptors.DescriptorProperties.PRIMARY_KEY_NAME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;
import static org.apache.flink.table.descriptors.FormatDescriptorValidator.FORMAT;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_DATA_TYPE;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_FROM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;
import static org.apache.flink.table.descriptors.StdIOValidator.IDENTIFIER;
import static org.apache.flink.table.descriptors.StreamTableDescriptorValidator.UPDATE_MODE;

/**
 * StdIOLegacyTableFactory for standard input/output stream.
 */
@Deprecated
public class StdIOLegacyTableFactory implements StreamTableSourceFactory<Row>, StreamTableSinkFactory<Row> {
	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, IDENTIFIER);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();
		properties.add(UPDATE_MODE);

		properties.add(SCHEMA + ".#." + SCHEMA_DATA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);
		properties.add(SCHEMA + ".#." + SCHEMA_FROM);

		properties.add(SCHEMA + ".#.expr");

		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_ROWTIME);
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_EXPR);
		properties.add(SCHEMA + "." + WATERMARK + ".#." + WATERMARK_STRATEGY_DATA_TYPE);

		properties.add(SCHEMA + "." + PRIMARY_KEY_NAME);
		properties.add(SCHEMA + "." + PRIMARY_KEY_COLUMNS);

		properties.add(FORMAT + ".*");
		return properties;
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		new StdIOValidator().validate(descriptorProperties);
		return descriptorProperties;
	}

	@Override
	public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
		DescriptorProperties validatedProperties = getValidatedProperties(properties);

		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(validatedProperties.getTableSchema(SCHEMA));

		@SuppressWarnings("unchecked")
		DeserializationSchema<Row> deserializationSchema = TableFactoryService
				.find(DeserializationSchemaFactory.class, properties)
				.createDeserializationSchema(properties);

		return new StdIOLegacyTableSource(tableSchema, deserializationSchema);
	}

	@Override
	public StreamTableSink<Row> createStreamTableSink(Map<String, String> properties) {
		DescriptorProperties validatedProperties = getValidatedProperties(properties);
		TableSchema tableSchema = TableSchemaUtils.getPhysicalSchema(validatedProperties.getTableSchema(SCHEMA));
		return new StdIOLegacyTableSink(tableSchema);
	}
}
