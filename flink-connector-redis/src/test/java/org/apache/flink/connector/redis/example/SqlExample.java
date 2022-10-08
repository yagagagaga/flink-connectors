package org.apache.flink.connector.redis.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Objects;

/**
 * A easy example for flink-connector-shardedjedis.
 */
public class SqlExample {

	@FunctionHint(input = {@DataTypeHint(inputGroup = InputGroup.ANY)})
	public static class A extends ScalarFunction {
		public byte[] eval(Object a) {
			return Objects.toString(a).getBytes();
		}
	}
	
	@FunctionHint(input = {@DataTypeHint(inputGroup = InputGroup.ANY)})
	public static class B extends ScalarFunction {
		public String eval(Object a) {
			return Objects.toString(a);
		}
	}
	
	public static void main(String[] args) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
		env.setParallelism(1);
		
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		tableEnv.createTemporaryFunction("toBytes", new A());
		tableEnv.createTemporaryFunction("toString", new B());
		
		
	}
}
