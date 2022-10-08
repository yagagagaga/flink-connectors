package org.apache.flink.connector.redis.client;

import redis.clients.jedis.BinaryJedisCluster;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClusterConnectionHandler;
import redis.clients.jedis.JedisClusterInfoCache;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.jedis.util.JedisClusterCRC16;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class JedisClusterPipeline {

	/**
	 * 管道命令提交阈值
	 */
	private final int MAX_COUNT = 10000;
	/**
	 * Redis集群缓存信息对象 Jedis提供
	 */
	private JedisClusterInfoCache clusterInfoCache;
	/**
	 * Redis链接处理对象 继承于JedisClusterConnectionHandler,对其提供友好的调用方法 Jedis提供
	 */
	private JedisSlotBasedConnectionHandler connectionHandler;
	/**
	 * Redis集群操作对象 Jedis提供
	 */
	private JedisCluster jedisCluster;
	/**
	 * 存储获取的Jedis对象，用于统一释放对象
	 */
	private CopyOnWriteArrayList<Jedis> jedisList = new CopyOnWriteArrayList();
	/**
	 * 存储获取的Jedis连接池对象与其对应开启的管道，用于保证slot(哈希槽)对应的节点链接的管道只被开启一次
	 */
	private ConcurrentHashMap<JedisPool, Pipeline> pipelines = new ConcurrentHashMap<>();
	/**
	 * 存储每个开启的管道需要处理的命令（数据）数，当计数达到提交阈值时进行提交
	 */
	private ConcurrentHashMap<Pipeline, Integer> nums = new ConcurrentHashMap<>();
	/**
	 * 构造方法
	 * 通过JedisCluster获取JedisClusterInfoCache和JedisSlotBasedConnectionHandler
	 *
	 * @param jedisCluster
	 */
	public JedisClusterPipeline(JedisCluster jedisCluster) throws NoSuchFieldException, IllegalAccessException {
		this.jedisCluster = jedisCluster;

		this.connectionHandler = (JedisSlotBasedConnectionHandler) BinaryJedisCluster.class.getDeclaredField("connectionHandler").get(jedisCluster);
		this.clusterInfoCache = (JedisClusterInfoCache) JedisClusterConnectionHandler.class.getDeclaredField("cache").get(connectionHandler);
	}

	public void hsetByPipeline(String key, String field, String value) {
		Pipeline pipeline = getPipeline(key);
		pipeline.hset(key, field, value);
		nums.put(pipeline, nums.get(pipeline) + 1);
		this.maxSync(pipeline);
	}

	/**
	 * 释放获取的Jedis链接
	 * 释放的过程中会强制执行PipeLine sync
	 */
	public void releaseConnection() {
		jedisList.forEach(jedis -> jedis.close());
	}

	/**
	 * 获取JedisPool
	 * 第一次获取不到尝试刷新缓存的SlotPool再获取一次
	 *
	 * @param key
	 * @return
	 */
	private JedisPool getJedisPool(String key) {
		/** 通过key计算出slot */
		int slot = JedisClusterCRC16.getSlot(key);
		/** 通过slot获取到对应的Jedis连接池 */
		JedisPool jedisPool = clusterInfoCache.getSlotPool(slot);
		if (null != jedisPool) {
			return jedisPool;
		} else {
			/** 刷新缓存的SlotPool */
			connectionHandler.renewSlotCache();
			jedisPool = clusterInfoCache.getSlotPool(slot);
			if (jedisPool != null) {
				return jedisPool;
			} else {
				throw new JedisNoReachableClusterNodeException("No reachable node in cluster for slot " + slot);
			}
		}
	}

	/**
	 * 获取Pipeline对象
	 * 缓存在pipelines中，保证集群中同一节点的Pipeline只被开启一次
	 * 管道第一次开启，jedisList，pipelines，nums存入与该管道相关信息
	 *
	 * @param key
	 * @return
	 */
	private Pipeline getPipeline(String key) {
		JedisPool jedisPool = getJedisPool(key);
		/** 检查管道是否已经开启 */
		Pipeline pipeline = pipelines.get(jedisPool);
		if (null == pipeline) {
			Jedis jedis = jedisPool.getResource();
			pipeline = jedis.pipelined();
			jedisList.add(jedis);
			pipelines.put(jedisPool, pipeline);
			nums.put(pipeline, 0);
		}
		return pipeline;
	}

	/**
	 * 管道对应的命令计数，并在达到阈值时触发提交
	 * 提交后计数归零
	 *
	 * @param pipeline
	 * @return
	 */
	private void maxSync(Pipeline pipeline) {
		Integer num = nums.get(pipeline);
		if (null != num) {
			if (num % MAX_COUNT == 0) {
				pipeline.sync();
				nums.put(pipeline, 0);
			}
		}
	}
}
