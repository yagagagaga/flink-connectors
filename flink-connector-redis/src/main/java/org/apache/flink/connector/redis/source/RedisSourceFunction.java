package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.connector.redis.client.RedisProxy;
import org.apache.flink.connector.redis.client.RedisShardedProxy;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.IOUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

/**
 * RedisSourceFunction for Redis client.
 */
public class RedisSourceFunction<T> extends RichParallelSourceFunction<T> {

	private final RedisRecordProducer<T> producer;

	private final RedisOptions redisOptions;

	private final Map<Future<?>, RedisClientProxy> scanners = new ConcurrentHashMap<>();
	private volatile boolean running = true;

	private transient RedisClientProxy redisClient;

	private transient ExecutorService executorService;

	public RedisSourceFunction(
			RedisRecordProducer<T> producer,
			RedisOptions redisOptions) {
		this.producer = producer;
		this.redisOptions = redisOptions;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		final RuntimeContext ctx = getRuntimeContext();
		this.redisClient = redisOptions.createClient(ctx);
		if (redisClient != null) {
			if (redisClient instanceof RedisShardedProxy) {
				executorService = Executors.newWorkStealingPool(((RedisShardedProxy) redisClient).getAllShards().size());
			} else {
				executorService = Executors.newWorkStealingPool(1);
			}
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (executorService != null) {
			executorService.shutdown();
		}
		IOUtils.closeQuietly(redisClient);
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {

		if (redisClient == null || executorService == null) {
			return;
		}

		ScanParams scanParams = new ScanParams()
				.match(redisOptions.keyPattern())
				.count(1000);

		CompletionService<Object> cs = new ExecutorCompletionService<>(executorService);

		if (redisClient instanceof RedisShardedProxy) {
			for (Jedis j : ((RedisShardedProxy) redisClient).getAllShards()) {
				scanners.put(cs.submit(() -> j.scan(SCAN_POINTER_START_BINARY, scanParams)), new RedisProxy(j));
			}
		} else {
			scanners.put(cs.submit(() -> redisClient.scan(SCAN_POINTER_START_BINARY, scanParams)), redisClient);
		}

		while (!scanners.isEmpty() && running) {
			Future<Object> future = cs.take();
			RedisClientProxy j = scanners.remove(future);
			Object obj = future.get();
			if (obj instanceof ScanResult) {
				@SuppressWarnings("unchecked") final ScanResult<byte[]> scanResult = (ScanResult<byte[]>) obj;
				final List<byte[]> keys = scanResult.getResult();
				byte[] cursor = scanResult.getCursorAsBytes();
				scanners.put(cs.submit(
						() -> produce(sourceContext, j, keys, cursor, redisOptions.ignoreError())), j);
			} else if (obj instanceof byte[]) {
				final byte[] cursor = (byte[]) obj;
				if (!Arrays.equals(SCAN_POINTER_START_BINARY, cursor)) {
					scanners.put(cs.submit(() -> j.scan(cursor, scanParams)), j);
				}
			} else {
				throw new IllegalStateException(
						"It shouldn't happen! Here take " + obj + ", but expected ScanResult or byte[]");
			}
		}
	}

	private byte[] produce(
			SourceContext<T> sourceContext,
			RedisClientProxy client,
			List<byte[]> keys,
			byte[] cursor, boolean ignoreError) {
		if (keys != null && !keys.isEmpty()) {
			final List<T> results = producer.apply(keys, client, ignoreError);
			synchronized (sourceContext.getCheckpointLock()) {
				for (T t : results) {
					sourceContext.collect(t);
				}
			}
		}
		return cursor;
	}

	@Override
	public void cancel() {
		running = false;
	}
}
