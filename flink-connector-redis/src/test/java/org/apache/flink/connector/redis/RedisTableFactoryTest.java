package org.apache.flink.connector.redis;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.sinks.StreamTableSink;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_HOST;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PORT;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_TYPE_VALUE_REDIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test case for RedisTableFactory.
 */
public class RedisTableFactoryTest {

	private static final TableSchema SCHEMA = TableSchema.builder()
			.field("key", DataTypes.STRING())
			.field("value", DataTypes.STRING())
			.build();

	private static final String HOST = "localhost";
	private static final int PORT = 6379;
	private static final String DATA_STRUCTURE = "string";

	@Test
	public void testTableSource_create_notSupported() {
		Map<String, String> properties = getBasicProperties();
		try {
			TableFactoryService.find(StreamTableSourceFactory.class, properties)
					.createStreamTableSource(properties);
			fail();
		} catch (IllegalStateException e) {
			String expected = "Redis Table Source is not supported yet";
			String actual = e.getMessage();
			assertEquals(expected, actual);
		}
	}

	@Test
	public void testTableSink_create_success() {
		Map<String, String> config = new HashMap<>();
		config.put("max-flush-size", "1");
		RedisRetractTableSink expected = new RedisRetractTableSink(null, SCHEMA, null);

		Map<String, String> properties = getBasicProperties();
		properties.put("connector.properties.max-flush-size", "1");

		StreamTableSink<?> actual = TableFactoryService.find(StreamTableSinkFactory.class, properties)
				.createStreamTableSink(properties);

		assertEquals(expected, actual);
	}

	private Map<String, String> getBasicProperties() {
		Map<String, String> properties = new HashMap<>();

		properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_REDIS);
		properties.put(CONNECTOR_VERSION, "4");

		properties.put(CONNECTOR_HOST, HOST);
		properties.put(CONNECTOR_PORT, String.valueOf(PORT));
		properties.put(CONNECTOR_DATA_STRUCTURE, DATA_STRUCTURE);

		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(properties);
		descriptorProperties.putTableSchema("schema", SCHEMA);

		return new HashMap<>(descriptorProperties.asMap());
	}
}
