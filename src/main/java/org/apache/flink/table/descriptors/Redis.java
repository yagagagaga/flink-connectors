package org.apache.flink.table.descriptors;

import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_CHANNEL_PATTERN;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_HOST;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_KEY_POS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MAX_FLUSH_SIZE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PORT;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_TYPE_VALUE_REDIS;

public class Redis extends ConnectorDescriptor {

    private final DescriptorProperties properties = new DescriptorProperties();

    public Redis() {
        super(CONNECTOR_TYPE_VALUE_REDIS, 1, true);
    }

    public Redis version(String version) {
        properties.putString(CONNECTOR_VERSION, version);
        return this;
    }

    public Redis host(String host) {
        properties.putString(CONNECTOR_HOST, host);
        return this;
    }

    public Redis port(int port) {
        properties.putInt(CONNECTOR_PORT, port);
        return this;
    }

    public Redis dataStructure(String type) {
        properties.putString(CONNECTOR_DATA_STRUCTURE, type);
        return this;
    }

    public Redis keyPos(int pos) {
        properties.putInt(CONNECTOR_KEY_POS, pos);
        return this;
    }
  
    public Redis channelPattern(String channelPattern) {
        properties.putString(CONNECTOR_CHANNEL_PATTERN, channelPattern);
        return this;
    }

    public Redis maxFlushSize(int maxFlushSize) {
        properties.putInt(CONNECTOR_MAX_FLUSH_SIZE, maxFlushSize);
        return this;
    }

    public Redis properties(Map<String, String> config) {
        properties.putProperties(config);
        return this;
    }

    @Override
    protected Map<String, String> toConnectorProperties() {
        return properties.asMap();
    }
}
