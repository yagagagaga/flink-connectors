package org.apache.flink.connector.redis.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

import static org.apache.flink.connector.redis.example.RedisFlinkTestConstants.ACCOUNTS;

/**
 * An example for flink-redis-channel.
 */
public class RedisReadChannelExample {

	public static void main(String[] args) throws Exception {
		final Thread thread = new Thread(() -> {
			final Jedis jedis = new Jedis("localhost", 6379);
			final Pipeline pipelined = jedis.pipelined();
			while (true) {
				try {
					for (String account : ACCOUNTS) {
						pipelined.publish("test_channel", account);
					}
					pipelined.sync();
					Thread.sleep(1000);
				} catch (Exception e) {
					e.printStackTrace();
					try {
						pipelined.close();
						jedis.close();
					} catch (IOException ignored) {
					}
					break;
				}
			}
		}, "redis-channel-producer");
		thread.setDaemon(true);
		thread.start();

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
				env, EnvironmentSettings.newInstance().useBlinkPlanner().build());

		String ddl = "" +
				"CREATE TABLE t_redis (" +
				"  `name` VARCHAR, " +
				"  `age` INT, " +
				"  `year_of_birth` AS YEAR(CURRENT_DATE) - `age`, " +
				"  `account` VARCHAR, " +
				"  `balance` VARCHAR, " +
				"  `date_of_registration` VARCHAR, " +
				"  `ts` AS TO_TIMESTAMP(`date_of_registration`) " +
				") WITH (" +
				"  'format.type' = 'csv'," +
				"  'connector.type' = 'redis'," +
				"  'connector.version' = 'universal'," +
				"  'connector.host' = 'localhost'," +
				"  'connector.port' = '6379'," +
				"  'connector.data-structure' = 'subscribe'," +
				"  'connector.channel-pattern' = 'test_channel'" +
				")";
		tableEnv.sqlUpdate(ddl);

		String ddl2 = "" +
				"CREATE TABLE t_redis_out (" +
				"  `name` VARCHAR, " +
				"  `year_of_birth` BIGINT, " +
				"  `account` VARCHAR, " +
				"  `balance` VARCHAR, " +
				"  `ts` TIMESTAMP(3) " +
				") WITH (" +
				"  'format.type' = 'json'," +
				"  'connector.type' = 'redis'," +
				"  'connector.version' = 'universal'," +
				"  'connector.host' = 'localhost'," +
				"  'connector.port' = '6379'," +
				"  'connector.data-structure' = 'publish'," +
				"  'connector.channel-pattern' = 'accounts'," +
				"  'connector.key-pos' = '0'" +
				")";
		tableEnv.sqlUpdate(ddl2);

		String sql = "" +
				"INSERT INTO t_redis_out " +
				"SELECT" +
				"  `name`," +
				"  `year_of_birth`," +
				"  `account`," +
				"  `balance`," +
				"  `ts`" +
				"FROM" +
				"  t_redis";
		tableEnv.sqlUpdate(sql);

		tableEnv.execute("redis-channel-example");
	}
}
