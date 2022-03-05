package org.apache.flink.connector.redis;

import org.apache.flink.table.descriptors.DescriptorProperties;

import java.io.Serializable;
import java.util.Map;

import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_CHANNEL_PATTERN;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_DATA_STRUCTURE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_HOST;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_KEY_POS;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_MAX_FLUSH_SIZE;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PORT;
import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_PROPERTIES;

public class RedisOptions implements Serializable {
    public final String host;
    public final int port;
    public final String dataStructure;
    public final int keyPos;
    public final String channelPattern;
    public final int maxFlushSize;
    public final Map<String, String> config;

    private RedisOptions(
            String host,
            int port,
            String dataStructure,
            int keyPos,
            String channelPattern,
            int maxFlushSize,
            Map<String, String> config) {
        this.host = host;
        this.port = port;
        this.dataStructure = dataStructure;
        this.keyPos = keyPos;
        this.channelPattern = channelPattern;
        this.maxFlushSize = maxFlushSize;
        this.config = config;
    }

    public static RedisOptions getRedisOptions(DescriptorProperties descriptorProperties) {
        String host = descriptorProperties.getString(CONNECTOR_HOST);
        int port = descriptorProperties.getInt(CONNECTOR_PORT);
        String dataStructure = descriptorProperties.getString(CONNECTOR_DATA_STRUCTURE);
        int keyPos = descriptorProperties.getOptionalInt(CONNECTOR_KEY_POS).orElse(-1);
        String channelPattern = descriptorProperties.getOptionalString(CONNECTOR_CHANNEL_PATTERN).orElse(null);
        int maxFlushSize = descriptorProperties.getOptionalInt(CONNECTOR_MAX_FLUSH_SIZE).orElse(1);
        Map<String, String> config = descriptorProperties.getPropertiesWithPrefix(CONNECTOR_PROPERTIES);

        return new RedisOptions(host, port, dataStructure, keyPos, channelPattern, maxFlushSize, config);
    }
}
