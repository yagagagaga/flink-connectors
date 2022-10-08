package org.apache.flink.connector.redis.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class MasterListener extends Thread {

	/**
	 * @see JedisSentinelPool#initPool(HostAndPort)
	 */
	public static final String INIT_POOL_METHOD_NAME = "initPool";
	public static final Method INIT_POOL_METHOD;

	private static final Logger LOG = LoggerFactory.getLogger(MasterListener.class);

	static {
		try {
			Method poolMethod = JedisSentinelPool.class.getDeclaredMethod(INIT_POOL_METHOD_NAME,
					HostAndPort.class);
			poolMethod.setAccessible(true);
			INIT_POOL_METHOD = poolMethod;
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	private final AtomicBoolean running = new AtomicBoolean(false);
	private final Map<String, List<JedisSentinelPool>> masters;
	private final String host;
	private final int port;
	private final int sentinelConnectionTimeout;
	private final int sentinelSoTimeout;
	private final String sentinelUser;
	private final String sentinelPassword;
	private final String sentinelClientName;
	private final Long subscribeRetryWaitTimeMillis;
	private volatile Jedis j = null;

	public MasterListener(
			Map<String, List<JedisSentinelPool>> masters,
			String host,
			int port,
			int sentinelConnectionTimeout,
			int sentinelSoTimeout,
			String sentinelUser,
			String sentinelPassword,
			String sentinelClientName,
			Long subscribeRetryWaitTimeMillis) {
		super(String.format("MasterListener-%d-masters", masters.size()));
		this.masters = masters;
		this.host = host;
		this.port = port;
		this.sentinelConnectionTimeout = sentinelConnectionTimeout;
		this.sentinelSoTimeout = sentinelSoTimeout;
		this.sentinelUser = sentinelUser;
		this.sentinelPassword = sentinelPassword;
		this.sentinelClientName = sentinelClientName;
		this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
	}

	public MasterListener(
			Map<String, List<JedisSentinelPool>> masters,
			String host,
			int port,
			int sentinelConnectionTimeout,
			int sentinelSoTimeout,
			String sentinelUser,
			String sentinelPassword,
			String sentinelClientName) {
		this(
				masters,
				host,
				port,
				sentinelConnectionTimeout,
				sentinelSoTimeout,
				sentinelUser,
				sentinelPassword,
				sentinelClientName,
				5000L);
	}

	public void run() {
		running.set(true);
		while (running.get()) {
			try {
				if (!running.get()) {
					break;
				}
				j = new Jedis(host, port, sentinelConnectionTimeout, sentinelSoTimeout);
				if (sentinelUser != null) {
					j.auth(sentinelUser, sentinelPassword);
				} else if (sentinelPassword != null) {
					j.auth(sentinelPassword);
				}

				if (sentinelClientName != null) {
					j.clientSetname(sentinelClientName);
				}

				// code for active refresh
				masters.forEach((masterName, pools) -> {
					List<String> masterAddr = j.sentinelGetMasterAddrByName(masterName);
					if (masterAddr == null || (masterAddr.size() != 2)) {
						LOG.warn(
								"Can not get master addr, master name: {}. Sentinel: {}:{}.",
								masterName,
								host,
								port);
					} else {
						initPools(pools, toHostAndPort(masterAddr));
					}
				});

				j.subscribe(
						new JedisPubSub() {
							@Override
							public void onMessage(String channel, String message) {
								LOG.debug("Sentinel {}:{} published: {}.", host, port, message);
								String[] switchMasterMsg = message.split(" ");
								if (switchMasterMsg.length > 4) {
									List<JedisSentinelPool> pools = masters.get(switchMasterMsg[0]);
									if (pools != null) {
										initPools(
												pools,
												toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4])));
									} else {
										LOG.debug(
												"Ignoring message on +switch-master for master name {}",
												switchMasterMsg[0]);
									}
								} else {
									LOG.error(
											"Invalid message received on Sentinel {}:{} " +
													"on channel +switch-master: {}",
											host,
											port,
											message);
								}
							}
						},
						"+switch-master");
			} catch (JedisException e) {
				if (running.get()) {
					LOG.error(
							"Lost connection to Sentinel at {}:{}. Sleeping 5000ms and retrying.",
							host,
							port,
							e);
					try {
						Thread.sleep(subscribeRetryWaitTimeMillis);
					} catch (InterruptedException e1) {
						LOG.error("Sleep interrupted: ", e1);
					}
				} else {
					LOG.debug("Unsubscribing from Sentinel at {}:{}", host, port);
				}
			} finally {
				if (j != null) {
					j.close();
				}
			}
		}
	}

	public void shutdown() {
		try {
			LOG.debug("Shutting down listener on {}:{}", host, port);
			running.set(false);
			// This isn't good, the Jedis object is not thread safe
			if (j != null) {
				j.disconnect();
			}
		} catch (Exception e) {
			LOG.error("Caught exception while shutting down: ", e);
		}
	}

	private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
		String host = getMasterAddrByNameResult.get(0);
		int port = Integer.parseInt(getMasterAddrByNameResult.get(1));
		return new HostAndPort(host, port);
	}

	public void initPools(List<JedisSentinelPool> pools, HostAndPort master) {
		pools.forEach(pool -> {
			try {
				MasterListener.INIT_POOL_METHOD.invoke(pool, master);
			} catch (Exception e) {
				LOG.error("Can not init pool " + pool.getCurrentHostMaster(), e);
			}
		});
	}

}
