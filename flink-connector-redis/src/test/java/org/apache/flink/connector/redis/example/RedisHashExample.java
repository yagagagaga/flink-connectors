package org.apache.flink.connector.redis.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.connector.redis.table.RedisTableFactory.IDENTIFIER;

public class RedisHashExample {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		String host = "192.168.1.140";
		String port = "6379";

		tableEnv.executeSql(""
				+ "CREATE TEMPORARY TABLE t_input (\n"
				+ "  f1 STRING,\n"
				+ "  f2 STRING,\n"
				+ "  f3 STRING\n"
				+ ") WITH (\n"
				+ "  'connector' = 'datagen',\n"
				+ "  'rows-per-second'='1',\n"
				+ "  'fields.f1.length'='3',\n"
				+ "  'fields.f2.length'='4',\n"
				+ "  'fields.f3.length'='10'\n"
				+ ")");

		tableEnv.executeSql(""
				+ "CREATE TABLE t_redis_output (\n"
				+ "` key`   STRING,\n"
				+ "` value` MAP<STRING, STRING>\n"
				+ ") WITH (\n"
				+ "  'connector' = '" + IDENTIFIER + "',\n"
				+ "  'redis.host' = '" + host + "',\n"
				+ "  'redis.port' = '" + port + "',\n"
				+ "  'redis.data-type' = 'hash',\n"
				+ "  'redis.key-pattern' = '*'\n"
				+ ")");

		tableEnv.executeSql("" +
				"INSERT INTO t_redis_output\n" +
						"SELECT CONCAT('hash_', f1) AS `key`, MAP[f2, f3] AS `value` FROM t_input");

		tableEnv.executeSql(""
				+ "CREATE TABLE t_redis_input (\n"
				+ "` key`   STRING,\n"
				+ "` value` MAP<STRING, STRING>\n"
				+ ") WITH (\n"
				+ "  'connector' = '" + IDENTIFIER + "',\n"
				+ "  'redis.host' = '" + host + "',\n"
				+ "  'redis.port' = '" + port + "',\n"
				+ "  'redis.data-type' = 'hash',\n"
				+ "  'redis.key-pattern' = 'hash_*'\n"
				+ ")");

		tableEnv.executeSql("SELECT * FROM t_redis_input").print();
	}
}
