package org.apache.flink.connector.redis.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Redis;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.connector.redis.example.RedisFlinkTestConstants.WORDS;
import static org.apache.flink.runtime.util.jartestprogram.WordCountWithInnerClass.Tokenizer;

/**
 * An example for flink-redis-write.
 */
public class RedisWriteStreamExample {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
				env, EnvironmentSettings.newInstance().useBlinkPlanner().build());

		final DataStream<Tuple2<String, Integer>> wordsDS = env.fromElements(WORDS)
				.flatMap(new Tokenizer())
				.keyBy(0)
				.sum(1);

		tableEnv.createTemporaryView("t_words", wordsDS, "word, count");

		final Redis connectorDescriptor = new Redis()
				.version("universal")
				.host("localhost")
				.port(6379)
				.dataStructure("string")
				.keyPos(0);
		final Schema schema = new Schema()
				.field("key", DataTypes.STRING())
				.field("value", DataTypes.INT());
		final FormatDescriptor format = new Json();
		tableEnv.connect(connectorDescriptor)
				.withSchema(schema)
				.withFormat(format)
				.createTemporaryTable("t_redis");
		tableEnv.from("t_words").insertInto("t_redis");

		// or use SQL
//		String ddl = "" +
//				"CREATE TABLE t_redis (" +
//				"  `key` VARCHAR, " +
//				"  `value` INT " +
//				") WITH (" +
//				"  'format.type' = 'json'," +
//				"  'connector.type' = 'redis'," +
//				"  'connector.version' = 'universal'," +
//				"  'connector.host' = 'localhost'," +
//				"  'connector.port' = '6379'," +
//				"  'connector.data-structure' = 'string'," +
//				"  'connector.key-pos' = '1'," +
//				"  'connector.value-include-key' = 'true'" +
//				")";
//		tableEnv.sqlUpdate(ddl);
//		String dql = "INSERT INTO t_redis SELECT * FROM t_words";
//		tableEnv.sqlUpdate(dql);

		tableEnv.execute("Flink Redis Sink Example");
	}
}
