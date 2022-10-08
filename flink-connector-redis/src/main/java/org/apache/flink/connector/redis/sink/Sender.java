package org.apache.flink.connector.redis.sink;

import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.connector.redis.options.RedisOptions;
import org.apache.flink.connector.redis.util.DoubleBuffer;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.PipelineBase;
import redis.clients.jedis.ShardedJedisPipeline;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Sender<T> extends Thread implements Thread.UncaughtExceptionHandler {

	private static final Map<RedisOptions, Sender<?>> CACHE = new ConcurrentHashMap<>();
	private static final AtomicInteger ID = new AtomicInteger(1);
	private static final Logger LOG = LoggerFactory.getLogger(Sender.class);

	private final RedisRecordConsumer<T> consumer;
	private final boolean ignoreError;
	private final long timeout;
	private final DoubleBuffer<T> buffer = new DoubleBuffer<>();
	private final RedisClientProxy redisClient;

	private volatile boolean running = true;

	public Sender(RedisRecordConsumer<T> consumer, RedisClientProxy redisClient, RedisOptions redisOptions) {
		super("ShardedJedis-Sender" + ID.getAndIncrement());
		setDaemon(true);
		setUncaughtExceptionHandler(this);

		this.consumer = consumer;
		this.ignoreError = redisOptions.ignoreError();
		this.timeout = redisOptions.lingerMs();
		this.redisClient = redisClient;
		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
	}

	public synchronized static <T> Sender<T> getOrCreate(
			RedisRecordConsumer<T> consumer,
			RedisClientProxy redisClient,
			RedisOptions redisOptions) {
		if (redisClient == null) {
			return null;
		}
		@SuppressWarnings("unchecked")
		Sender<T> sender = (Sender<T>) CACHE.get(redisOptions);
		if (sender == null) {
			sender = new Sender<>(consumer, redisClient, redisOptions);
			sender.start();
			CACHE.put(redisOptions, sender);
		}

		return sender;
	}

	@Override
	public void run() {
		if (!running) {
			return;
		}

		while (running) {
			final long now = System.nanoTime();

			final List<T> flush = buffer.flush();
			if (!flush.isEmpty()) {
				PipelineBase pipelined = redisClient.pipelined();
				for (T t : flush) {
					try {
						consumer.apply(t, pipelined);
					} catch (Exception e) {
						if (ignoreError) {
							LOG.error(e.getMessage(), e);
						} else {
							throw e;
						}
					}
				}
				try {
					if (pipelined instanceof ShardedJedisPipeline) {
						((ShardedJedisPipeline) pipelined).sync();
					} else if (pipelined instanceof Pipeline) {
						((Pipeline) pipelined).sync();
					} else {
						pipelined.getClass().getMethod("sync").invoke(pipelined);
					}
				} catch (Exception e) {
					if (ignoreError) {
						LOG.error(e.getMessage(), e);
					} else {
						throw new RuntimeException(e);
					}
				}
			}

			long timeConsuming = (System.nanoTime() - now) / 1_000_000;
			long timeToSleep = timeout - timeConsuming;
			if (timeToSleep > 0) {
				try {
					sleep(timeConsuming);
				} catch (InterruptedException e) {
					interrupt();
				}
			}
		}
	}

	public void send(T value) {
		buffer.add(value);
	}

	private void close() {
		running = false;
		IOUtils.closeQuietly(redisClient);
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		close();
	}
}
