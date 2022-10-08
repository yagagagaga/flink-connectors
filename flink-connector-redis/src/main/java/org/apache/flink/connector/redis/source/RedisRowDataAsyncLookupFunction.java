package org.apache.flink.connector.redis.source;

import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import redis.clients.jedis.Tuple;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.redis.options.RedisDataType.HASH;
import static org.apache.flink.connector.redis.options.RedisDataType.SORTED_SET;

public class RedisRowDataAsyncLookupFunction extends AsyncTableFunction<RowData> {
	private final RedisOptions redisOptions;
	private final RedisRecordProducer<RowData> producer;
	private transient RedisClientProxy client;

	private transient Cache<RowData, Collection<RowData>> cache;
	private transient ExecutorService executor;

	public RedisRowDataAsyncLookupFunction(RedisOptions redisOptions, RedisRecordProducer<RowData> producer) {
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

		executor = Executors.newFixedThreadPool(1);
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

	public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object key) {
		if (key == null) {
			return;
		}
		final byte[] keyBinary = getKey(key);
		switch (redisOptions.dataType()) {
			case STRING:
			case LIST:
			case SET:
			case SORTED_SET:
			case HASH:
				CompletableFuture.supplyAsync(() -> {
					RowData cacheKey = GenericRowData.of(key);
					if (cache != null) {
						Collection<RowData> cachedRows = cache.getIfPresent(cacheKey);
						if (cachedRows != null) {
							return cachedRows;
						}
					}

					final List<RowData> rows =
							producer.apply(Collections.singletonList(keyBinary), client, redisOptions.ignoreError());
					if (rows == null) {
						return Collections.<RowData>emptyList();
					} else {
						if (cache != null) {
							cache.put(cacheKey, rows);
						}
						return rows;
					}
				}, executor).thenAccept(resultFuture::complete);
				break;
			default:
				throw new IllegalStateException("Unsupported " + redisOptions.dataType() + " in AsyncLookupFunction.");
		}
	}

	public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object key, Object field) {
		if (key == null || field == null) {
			return;
		}
		if (redisOptions.dataType() != HASH) {
			throw new IllegalStateException("Only support for Redis hash if you has two lookup key.");
		}
		CompletableFuture.supplyAsync(() -> {
			RowData cacheKey = GenericRowData.of(key, field);
			if (cache != null) {
				Collection<RowData> cachedRows = cache.getIfPresent(cacheKey);
				if (cachedRows != null) {
					return cachedRows;
				}
			}

			byte[] keyBinary = getKey(key);
			byte[] fieldBinary = getKey(field);
			final byte[] valueBinary = client.hget(keyBinary, fieldBinary);
			if (valueBinary == null) {
				return Collections.<RowData>emptyList();
			}

			RowData rowData;
			if (key instanceof StringData) {
				rowData = GenericRowData.of(key, field, StringData.fromBytes(valueBinary));
			} else {
				rowData = GenericRowData.of(key, field, valueBinary);
			}
			final Collection<RowData> values = Collections.singletonList(rowData);
			if (cache != null) {
				cache.put(cacheKey, values);
			}
			return values;
		}, executor).thenAccept(resultFuture::complete);
	}

	public void eval(CompletableFuture<Collection<RowData>> resultFuture, Object key, Double min, Double max) {
		if (key == null) {
			return;
		}
		if (redisOptions.dataType() != SORTED_SET) {
			throw new IllegalStateException("Only support for Redis sort_set if you has three lookup key.");
		}
		CompletableFuture.supplyAsync(() -> {
			RowData cacheKey = GenericRowData.of(key, min, max);
			if (cache != null) {
				Collection<RowData> cachedRows = cache.getIfPresent(cacheKey);
				if (cachedRows != null) {
					return cachedRows;
				}
			}

			byte[] keyBinary = getKey(key);
			final Set<Tuple> tuples = client.zrangeByScoreWithScores(keyBinary, min, max);
			if (tuples == null || tuples.isEmpty()) {
				return Collections.<RowData>emptyList();
			}

			final Collection<RowData> rowDatas = ((RedisSortedSetProducer) producer).convert(keyBinary, tuples);
			if (cache != null) {
				cache.put(cacheKey, rowDatas);
			}
			return rowDatas;
		}, executor).thenAccept(resultFuture::complete);
	}

	@Override
	public void close() throws Exception {
		super.close();
		executor.shutdown();
		cache.cleanUp();
	}
}
