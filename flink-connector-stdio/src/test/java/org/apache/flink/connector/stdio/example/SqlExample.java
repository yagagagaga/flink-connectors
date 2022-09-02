package org.apache.flink.connector.stdio.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * A easy example for flink-connector-stdio.
 */
public class SqlExample {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		tableEnv.executeSql(""
				+ "CREATE TABLE t_input (\n"
				+ "  id   STRING,\n"
				+ "  name STRING,\n"
				+ "  age  INT\n"
				+ ") WITH (\n"
				+ "  'connector.type' = 'std.io',\n"
				+ "  'format.type' = 'csv'\n"
				+ ")");
		tableEnv.executeSql(""
				+ "CREATE TABLE t_output (\n"
				+ "  id   STRING,\n"
				+ "  name STRING,\n"
				+ "  age  INT\n"
				+ ") WITH (\n"
				+ "  'connector.type' = 'std.io',\n"
				+ "  'format.type' = 'csv'\n"
				+ ")");
		tableEnv.executeSql("INSERT INTO t_output SELECT * FROM t_input");
	}
}
