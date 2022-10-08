package org.apache.flink.connector.redis.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.connector.redis.table.RedisTableFactory.IDENTIFIER;

public class RedisStringExample {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		String host = "192.168.1.140";
		String port = "6379";

		tableEnv.executeSql("" 
				+ "CREATE TEMPORARY TABLE t_input (\n" 
				+ "  f1 STRING,\n" 
				+ "  f2 STRING\n" 
				+ ") WITH (\n" 
				+ "  'connector' = 'datagen',\n" 
				+ "  'rows-per-second'='1',\n" 
				+ "  'fields.f1.length'='3',\n" 
				+ "  'fields.f2.length'='4'\n" 
				+ ")");

		tableEnv.executeSql(""
				+ "CREATE TABLE t_redis_output (\n"
				+ "` key`   STRING,\n"
				+ "` value` STRING\n"
				+ ") WITH (\n"
				+ "  'connector' = '" + IDENTIFIER + "',\n"
				+ "  'redis.host' = '" + host + "',\n"
				+ "  'redis.port' = '" + port + "',\n"
				+ "  'redis.data-type' = 'string',\n"
				+ "  'redis.key-pattern' = '*'\n"
				+ ")");

		tableEnv.executeSql("" 
						+ "INSERT INTO t_redis_output " 
						+ "SELECT CONCAT('string_', f1) AS `key`, f2 AS `value` FROM t_input");

		tableEnv.executeSql(""
				+ "CREATE TABLE t_redis_input (\n"
				+ "` key`   STRING,\n"
				+ "` value` STRING\n"
				+ ") WITH (\n"
				+ "  'connector' = '" + IDENTIFIER + "',\n"
				+ "  'redis.host' = '" + host + "',\n"
				+ "  'redis.port' = '" + port + "',\n"
				+ "  'redis.data-type' = 'string',\n"
				+ "  'redis.key-pattern' = 'string_*'\n"
				+ ")");

		tableEnv.executeSql("SELECT * FROM t_redis_input").print();
	}
}
