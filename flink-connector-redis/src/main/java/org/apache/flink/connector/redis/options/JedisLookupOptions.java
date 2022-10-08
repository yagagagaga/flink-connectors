package org.apache.flink.connector.redis.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;
import java.time.Duration;

public final class JedisLookupOptions implements Serializable {

	public static final ConfigOption<Long> REDIS_LOOKUP_CACHE_MAX_ROWS = ConfigOptions
			.key("lookup.cache.max-rows")
			.longType()
			.defaultValue(-1L)
			.withDescription("the max number of rows of lookup cache, over this value, the oldest rows will " +
					"be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is " +
					"specified. Cache is not enabled as default.");
	public static final ConfigOption<Duration> REDIS_LOOKUP_CACHE_TTL = ConfigOptions
			.key("lookup.cache.ttl")
			.durationType()
			.defaultValue(Duration.ofSeconds(10))
			.withDescription("the cache time to live.");
	public static final ConfigOption<Boolean> REDIS_LOOKUP_ASYNC = ConfigOptions
			.key("lookup.async")
			.booleanType()
			.defaultValue(false)
			.withDescription("whether to use asynchronous.");

	private final long cacheMaxSize;
	private final long cacheExpireMs;
	private final boolean async;

	public JedisLookupOptions(ReadableConfig tableOptions) {
		this.cacheMaxSize = tableOptions.get(REDIS_LOOKUP_CACHE_MAX_ROWS);
		this.cacheExpireMs = tableOptions.get(REDIS_LOOKUP_CACHE_TTL).toMillis();
		this.async = tableOptions.get(REDIS_LOOKUP_ASYNC);
	}

	public long cacheMaxSize() {
		return cacheMaxSize;
	}

	public long cacheExpireMs() {
		return cacheExpireMs;
	}
	
	public boolean async() {
		return async;
	}
}
