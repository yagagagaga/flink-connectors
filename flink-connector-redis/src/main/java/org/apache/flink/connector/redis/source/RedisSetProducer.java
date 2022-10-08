package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.data.StringData.fromBytes;
import static org.apache.flink.types.RowKind.INSERT;

public class RedisSetProducer implements RedisRecordProducer<RowData> {

	private final ListRecordGenerator<byte[], RowData> mapper;

	public RedisSetProducer(boolean isStringType) {
		mapper = isStringType
				? this::convertStringListToRowData
				: this::convertBytesListToRowData;
	}

	@Override
	public List<RowData> apply(List<byte[]> keys, RedisClientProxy client, boolean ignoreError) {
		final Pipeline p = (Pipeline) client.pipelined();
		List<Tuple2<byte[], Response<Set<byte[]>>>> results = new ArrayList<>(keys.size());
		for (byte[] key : keys) {
			final Response<Set<byte[]>> response = p.smembers(key);
			results.add(Tuple2.of(key, response));
		}
		try {
			p.sync();
			return results.stream()
					.map(x -> mapper.generate(x.f0, x.f1.get()))
					.collect(Collectors.toList());
		} catch (JedisDataException e) {
			if (!ignoreError) {
				throw e;
			}
			List<RowData> res = new ArrayList<>(keys.size());
			for (byte[] key : keys) {
				try {
					res.add(mapper.generate(key, client.smembers(key)));
				} catch (JedisDataException jde) {
					LOG.error("smembers {}({}) failed!, because {}", Arrays.toString(key), new String(key), jde);
				}
			}
			return res;
		}
	}

	private RowData convertStringListToRowData(byte[] key, Set<byte[]> elems) {
		final StringData[] stringDatas = elems.stream().map(StringData::fromBytes).toArray(StringData[]::new);
		return GenericRowData.ofKind(INSERT, fromBytes(key), new GenericArrayData(stringDatas));
	}

	private RowData convertBytesListToRowData(byte[] key, Set<byte[]> elems) {
		final byte[][] bytesDatas = elems.toArray(new byte[0][]);
		return GenericRowData.ofKind(INSERT, key, new GenericArrayData(bytesDatas));
	}

	@FunctionalInterface
	public interface ListRecordGenerator<I, O> extends Function {
		O generate(I key, Set<byte[]> elems);
	}
}
