package org.apache.flink.connector.redis.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.connector.redis.table.RedisTableFactory.IDENTIFIER;

public class RedisPubSubExample {
	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		String host = "192.168.1.140";
		String port = "6379";

		tableEnv.executeSql("" +
				"CREATE TEMPORARY TABLE t_input (" +
				"  f_sequence INT," +
				"  f_random INT," +
				"  f_random_str STRING" +
				") WITH (" +
				"  'connector' = 'datagen'," +
				"  'rows-per-second'='1'," +
				"  'fields.f_sequence.kind'='sequence'," +
				"  'fields.f_sequence.start'='1'," +
				"  'fields.f_sequence.end'='1000'," +
				"  'fields.f_random.min'='1'," +
				"  'fields.f_random.max'='3'," +
				"  'fields.f_random_str.length'='10'" +
				")");

		tableEnv.executeSql(""
				+ "CREATE TABLE t_publish (\n"
				+ "  f_sequence INT,\n"
				+ "  f_random INT,\n"
				+ "  f_random_str STRING\n"
				+ ") WITH (\n"
				+ "  'connector' = '" + IDENTIFIER + "',\n"
				+ "  'redis.host' = '" + host + "',\n"
				+ "  'redis.port' = '" + port + "',\n"
				+ "  'redis.deploy-mode' = 'single',\n"
				+ "  'redis.data-type' = 'pubsub',\n"
				+ "  'redis.pubsub.publish-channel' = 'test',\n"
				+ "  'format' = 'csv'\n"
				+ ")");

		tableEnv.executeSql("INSERT INTO t_publish SELECT * FROM t_input");

		tableEnv.executeSql(""
				+ "CREATE TABLE t_subscribe (\n"
				+ "  f_sequence INT,\n"
				+ "  f_random INT,\n"
				+ "  f_random_str STRING\n"
				+ ") WITH (\n"
				+ "  'connector' = '" + IDENTIFIER + "',\n"
				+ "  'redis.host' = '" + host + "',\n"
				+ "  'redis.port' = '" + port + "',\n"
				+ "  'redis.deploy-mode' = 'single',\n"
				+ "  'redis.data-type' = 'pubsub',\n"
				+ "  'redis.pubsub.subscribe-patterns' = 'test',\n"
				+ "  'format' = 'csv'\n"
				+ ")");

		tableEnv.executeSql("select * from t_subscribe").print();
	}
}
