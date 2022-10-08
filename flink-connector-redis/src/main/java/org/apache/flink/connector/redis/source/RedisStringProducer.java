package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.types.RowKind.INSERT;

public class RedisStringProducer implements RedisRecordProducer<RowData> {

	private final RecordGenerator<byte[], RowData> recordMapper;

	public RedisStringProducer(boolean isStringType) {
		if (isStringType) {
			recordMapper = (k, v) -> GenericRowData.ofKind(
					INSERT,
					StringData.fromBytes(k),
					StringData.fromBytes(v));
		} else {
			recordMapper = (k, v) -> GenericRowData.ofKind(INSERT, k, v);
		}
	}

	@Override
	public List<RowData> apply(List<byte[]> keys, RedisClientProxy client, boolean ignoreError) {
		final Pipeline p = (Pipeline) client.pipelined();
		List<Tuple2<byte[], Response<byte[]>>> results = new ArrayList<>(keys.size());
		for (byte[] key : keys) {
			final Response<byte[]> response = p.get(key);
			results.add(Tuple2.of(key, response));
		}
		try {
			p.sync();
			return results.stream().map(x -> recordMapper.generate(x.f0, x.f1.get())).collect(Collectors.toList());
		} catch (JedisDataException e) {
			if (!ignoreError) {
				throw e;
			}
			List<RowData> res = new ArrayList<>(keys.size());
			for (byte[] key : keys) {
				try {
					res.add(recordMapper.generate(key, client.get(key)));
				} catch (JedisDataException jde) {
					LOG.error("GET {}({}) failed!, because {}", Arrays.toString(key), new String(key), jde);
				}
			}
			return res;
		}
	}

	@FunctionalInterface
	public interface RecordGenerator<I, O> extends Function {
		O generate(I key, I value);
	}
}
