package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.data.StringData.fromBytes;
import static org.apache.flink.types.RowKind.INSERT;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

public class RedisSortedSetProducer implements RedisRecordProducer<RowData> {

	private final RedisRecordProducer<RowData> delegate;
	private final RecordGenerator<byte[], RowData> recordMapper;
	private final SortedSetRecordGenerator<byte[], RowData> hashRecordMapper;

	public RedisSortedSetProducer(boolean isStringType, boolean isArrayStructure) {

		this.recordMapper = isArrayStructure
				? null : isStringType
				? this::convertStringTupleToRowData
				: this::convertBytesTupleToRowData;

		this.hashRecordMapper = !isArrayStructure
				? null : isStringType
				? this::convertStringTupleToRowData
				: this::convertBytesTupleToRowData;

		if (recordMapper == null) {
			delegate = (keys, client, ignoreError) -> {
				final Pipeline p = (Pipeline) client.pipelined();
				List<Tuple2<byte[], Response<Set<Tuple>>>> results = new ArrayList<>(keys.size());
				for (byte[] key : keys) {
					final Response<Set<Tuple>> response = p.zrangeWithScores(key, 0, -1);
					results.add(Tuple2.of(key, response));
				}
				try {
					p.sync();
					return results.stream()
							.map(x -> hashRecordMapper.generate(x.f0, x.f1.get()))
							.collect(Collectors.toList());
				} catch (JedisDataException e) {
					if (!ignoreError) {
						throw e;
					}
					List<RowData> res = new ArrayList<>(keys.size());
					for (byte[] key : keys) {
						try {
							res.add(hashRecordMapper.generate(key, client.zrangeWithScores(key, 0, -1)));
						} catch (JedisDataException jde) {
							LOG.error("zrangeWithScores {}({}) failed!, because {}", Arrays.toString(key), new String(key), jde);
						}
					}
					return res;
				}
			};
		} else {
			delegate = (keys, client, ignoreError) -> {
				ScanParams scanParams = new ScanParams().count(1000);
				List<RowData> res = new ArrayList<>(keys.size());
				for (byte[] key : keys) {
					try {
						byte[] cursorAsBytes = SCAN_POINTER_START_BINARY;
						do {
							final ScanResult<Tuple> zscan = client.zscan(key, cursorAsBytes, scanParams);
							for (Tuple tuple : zscan.getResult()) {
								RowData r = recordMapper.generate(key, tuple.getScore(), tuple.getBinaryElement());
								res.add(r);
							}
							cursorAsBytes = zscan.getCursorAsBytes();
						} while (!Arrays.equals(cursorAsBytes, SCAN_POINTER_START_BINARY));
					} catch (JedisDataException jde) {
						if (!ignoreError) {
							throw jde;
						}
						LOG.error("zscan {}({}) failed!, because {}", Arrays.toString(key), new String(key), jde);
					}
				}
				return res;
			};
		}
	}

	private RowData convertStringTupleToRowData(byte[] k, double s, byte[] v) {
		return GenericRowData.ofKind(INSERT, fromBytes(k), s, fromBytes(v));
	}

	private RowData convertBytesTupleToRowData(byte[] k, double s, byte[] v) {
		return GenericRowData.ofKind(INSERT, k, s, v);
	}

	private RowData convertStringTupleToRowData(byte[] k, Set<Tuple> s) {
		Object[] elems = new GenericRowData[s.size()];
		int i = 0;
		for (Tuple tuple : s) {
			elems[i++] = GenericRowData.of(tuple.getScore(), fromBytes(tuple.getBinaryElement()));
		}
		return GenericRowData.ofKind(
				INSERT, fromBytes(k), new GenericArrayData(elems));
	}

	private RowData convertBytesTupleToRowData(byte[] k, Set<Tuple> s) {
		Object[] elems = new GenericRowData[s.size()];
		int i = 0;
		for (Tuple tuple : s) {
			elems[i++] = GenericRowData.of(tuple.getScore(), tuple.getBinaryElement());
		}
		return GenericRowData.ofKind(INSERT, k, new GenericArrayData(elems));
	}
	
	public Collection<RowData> convert(byte[] key, Set<Tuple> values) {
		if (recordMapper != null) {
			return values.stream()
					.map(x -> recordMapper.generate(key, x.getScore(), x.getBinaryElement()))
					.collect(Collectors.toList());
		} else if (hashRecordMapper != null) {
			return Collections.singletonList(hashRecordMapper.generate(key, values));
		} else {
			throw new IllegalStateException("It should not happen!");
		}
	}
	
	@Override
	public List<RowData> apply(List<byte[]> keys, RedisClientProxy client, boolean ignoreError) {
		return delegate.apply(keys, client, ignoreError);
	}

	@FunctionalInterface
	public interface RecordGenerator<I, O> extends Function {
		O generate(I key, double score, I value);
	}

	@FunctionalInterface
	public interface SortedSetRecordGenerator<I, O> extends Function {
		O generate(I key, Set<Tuple> set);
	}
}
