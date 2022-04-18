package org.apache.flink.connector.redis.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;

import static org.apache.flink.connector.redis.example.RedisFlinkTestConstants.ACCOUNTS;
import static org.apache.flink.connector.redis.example.RedisFlinkTestConstants.DIM_USERS;

/**
 * An example for flink-redis-lookup.
 */
public class RedisLookupExample {

	public static void main(String[] args) throws Exception {

		final Thread thread = new Thread(() -> {
			final Jedis jedis = new Jedis("localhost", 6379);
			final Pipeline pipelined = jedis.pipelined();
			for (String user : DIM_USERS) {
				final String key = user.split(",")[0];
				pipelined.set(key, user);
			}
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
				"CREATE TABLE t_input (" +
				"  `name` VARCHAR, " +
				"  `age` INT, " +
				"  `account` VARCHAR, " +
				"  `balance` VARCHAR, " +
				"  `date_of_registration` VARCHAR, " +
				"  `proctime` as PROCTIME() " +
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
				"CREATE TABLE t_dim (" +
				"  `name` VARCHAR, " +
				"  `gender` VARCHAR, " +
				"  `country` VARCHAR " +
				") WITH (" +
				"  'format.type' = 'csv'," +
				"  'connector.type' = 'redis'," +
				"  'connector.version' = 'universal'," +
				"  'connector.host' = 'localhost'," +
				"  'connector.port' = '6379'," +
				"  'connector.data-structure' = 'string'" +
				")";
		tableEnv.sqlUpdate(ddl2);

		String sql = "" +
				"SELECT * " +
				"FROM" +
				"  t_input LEFT JOIN t_dim" +
				"  FOR SYSTEM_TIME AS OF t_input.proctime" +
				"  ON t_input.name = t_dim.name";
		final Table table = tableEnv.sqlQuery(sql);
		tableEnv.toAppendStream(table, Row.class)
				.print()
				.setParallelism(1);

		tableEnv.execute("Flink Redis Lookup Example");
	}
}
