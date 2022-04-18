package org.apache.flink.table.descriptors;

import java.util.Arrays;

/**
 * The validator for Redis.
 */
public class RedisValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_REDIS = "redis";
	public static final String CONNECTOR_HOST = "connector.host";
	public static final String CONNECTOR_PORT = "connector.port";
	public static final String CONNECTOR_DATA_STRUCTURE = "connector.data-structure";
	public static final String CONNECTOR_DATA_STRUCTURE_VALUE_STRING = "string";
	public static final String CONNECTOR_DATA_STRUCTURE_VALUE_LIST = "list";
	public static final String CONNECTOR_DATA_STRUCTURE_VALUE_SET = "set";
	public static final String CONNECTOR_DATA_STRUCTURE_VALUE_ZSET = "zset";
	public static final String CONNECTOR_DATA_STRUCTURE_VALUE_HASH = "hash";
	public static final String CONNECTOR_DATA_STRUCTURE_VALUE_PUBLISH = "publish";
	public static final String CONNECTOR_DATA_STRUCTURE_VALUE_SUBSCRIBE = "subscribe";
	public static final String CONNECTOR_KEY_POS = "connector.key-pos";
	public static final String CONNECTOR_CHANNEL_PATTERN = "connector.channel-pattern";
	public static final String CONNECTOR_MAX_FLUSH_SIZE = "connector.max-flush-size";
	public static final String CONNECTOR_PROPERTIES = "connector.properties";
	public static final String CONNECTOR_PROPERTIES_KEY = "key";
	public static final String CONNECTOR_PROPERTIES_VALUE = "value";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);

		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_REDIS, false);
		properties.validateString(CONNECTOR_HOST, false);
		properties.validateInt(CONNECTOR_PORT, false);
		properties.validateString(CONNECTOR_DATA_STRUCTURE, false);
		properties.validateEnumValues(CONNECTOR_DATA_STRUCTURE, false, Arrays.asList(
				CONNECTOR_DATA_STRUCTURE_VALUE_STRING,
				CONNECTOR_DATA_STRUCTURE_VALUE_LIST,
				CONNECTOR_DATA_STRUCTURE_VALUE_SET,
				CONNECTOR_DATA_STRUCTURE_VALUE_ZSET,
				CONNECTOR_DATA_STRUCTURE_VALUE_HASH,
				CONNECTOR_DATA_STRUCTURE_VALUE_PUBLISH,
				CONNECTOR_DATA_STRUCTURE_VALUE_SUBSCRIBE
		));
		properties.validateInt(CONNECTOR_KEY_POS, true);
		properties.validateString(CONNECTOR_CHANNEL_PATTERN, true);
		properties.validateInt(CONNECTOR_MAX_FLUSH_SIZE, true);
	}
}
