package org.apache.flink.connector.redis.source;

import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import redis.clients.jedis.Tuple;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.redis.options.RedisDataType.HASH;
import static org.apache.flink.connector.redis.options.RedisDataType.SORTED_SET;

public class RedisRowDataLookupFunction extends TableFunction<RowData> {
	private final RedisOptions redisOptions;
	private final RedisRecordProducer<RowData> producer;
	private transient RedisClientProxy client;

	private transient Cache<RowData, Collection<RowData>> cache;

	public RedisRowDataLookupFunction(RedisOptions redisOptions, RedisRecordProducer<RowData> producer) {
		this.redisOptions = redisOptions;
		this.producer = producer;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		client = redisOptions.createClient();

		final long cacheMaxSize = redisOptions.cacheMaxSize();
		final long cacheExpireMs = redisOptions.cacheExpireMs();
		this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
				.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
				.maximumSize(cacheMaxSize)
				.build();
	}

	private byte[] getKey(Object key) {
		if (key instanceof byte[]) {
			return (byte[]) key;
		} else if (key instanceof StringData) {
			return ((StringData) key).toBytes();
		} else {
			throw new IllegalArgumentException(""
					+ "You must make sure your KEY type is one of "
					+ "STRING/VARCHAR/BINARY/VARBINARY/BYTES.");
		}
	}

	public void eval(Object key) {
		if (key == null) {
			return;
		}
		RowData cacheKey = GenericRowData.of(key);
		if (cache != null) {
			Collection<RowData> cachedRows = cache.getIfPresent(cacheKey);
			if (cachedRows != null && !cachedRows.isEmpty()) {
				cachedRows.forEach(this::collect);
				return;
			}
		}
		switch (redisOptions.dataType()) {
			case STRING:
			case LIST:
			case SET:
			case SORTED_SET:
			case HASH:
				byte[] keyBinary = getKey(key);
				final List<RowData> apply =
						producer.apply(Collections.singletonList(keyBinary), client, redisOptions.ignoreError());
				if (cache != null) {
					cache.put(cacheKey, apply);
				}
				for (RowData rowData : apply) {
					collect(rowData);
				}
				break;
			default:
				throw new IllegalStateException("Unsupported " + redisOptions.dataType() + " in LookupFunction.");
		}
	}
	
	public void eval(Object key, Object field) {
		if (key == null || field == null) {
			return;
		}
		if (redisOptions.dataType() != HASH) {
			throw new IllegalStateException("Only support for Redis hash if you has two lookup key.");
		}

		RowData cacheKey = GenericRowData.of(key, field);
		if (cache != null) {
			Collection<RowData> cachedRows = cache.getIfPresent(cacheKey);
			if (cachedRows != null && !cachedRows.isEmpty()) {
				cachedRows.forEach(this::collect);
				return;
			}
		}

		byte[] keyBinary = getKey(key);
		byte[] fieldBinary = getKey(field);
		final byte[] valueBinary = client.hget(keyBinary, fieldBinary);
		if (valueBinary == null) {
			return;
		}

		RowData rowData;
		if (key instanceof StringData) {
			rowData = GenericRowData.of(key, field, StringData.fromBytes(valueBinary));
		} else {
			rowData = GenericRowData.of(key, field, valueBinary);
		}

		collect(rowData);

		if (cache != null) {
			cache.put(cacheKey, Collections.singleton(rowData));
		}
	}

	public void eval(Object key, Double min, Double max) {
		if (key == null) {
			return;
		}
		if (redisOptions.dataType() != SORTED_SET) {
			throw new IllegalStateException("Only support for Redis sort_set if you has three lookup key.");
		}
		RowData cacheKey = GenericRowData.of(key, min, max);
		if (cache != null) {
			Collection<RowData> cachedRows = cache.getIfPresent(cacheKey);
			if (cachedRows != null && !cachedRows.isEmpty()) {
				cachedRows.forEach(this::collect);
				return;
			}
		}

		byte[] keyBinary = getKey(key);
		final Set<Tuple> tuples = client.zrangeByScoreWithScores(keyBinary, min, max);
		final Collection<RowData> rowDatas = ((RedisSortedSetProducer) producer).convert(keyBinary, tuples);
		rowDatas.forEach(this::collect);
		if (cache != null) {
			cache.put(cacheKey, rowDatas);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
	}
}
