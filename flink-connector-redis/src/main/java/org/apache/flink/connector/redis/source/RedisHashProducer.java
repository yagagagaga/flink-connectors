package org.apache.flink.connector.redis.source;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.redis.client.RedisClientProxy;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.data.StringData.fromBytes;
import static org.apache.flink.types.RowKind.INSERT;
import static redis.clients.jedis.ScanParams.SCAN_POINTER_START_BINARY;

public class RedisHashProducer implements RedisRecordProducer<RowData> {

	private final RedisRecordProducer<RowData> delegate;

	public RedisHashProducer(boolean isStringType, boolean isMapStructure) {

		final RecordGenerator<byte[], RowData> recordMapper = isMapStructure
				? null : isStringType
				? this::convertStringTupleToRowData
				: this::convertBytesTupleToRowData;

		final MapRecordGenerator<byte[], RowData> hashRecordMapper = !isMapStructure
				? null : isStringType
				? this::convertStringMapToRowData
				: this::convertBytesMapToRowData;

		if (recordMapper == null) {
			delegate = (keys, client, ignoreError) -> {
				final Pipeline p = (Pipeline) client.pipelined();
				List<Tuple2<byte[], Response<Map<byte[], byte[]>>>> results = new ArrayList<>(keys.size());
				for (byte[] key : keys) {
					final Response<Map<byte[], byte[]>> response = p.hgetAll(key);
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
							res.add(hashRecordMapper.generate(key, client.hgetAll(key)));
						} catch (JedisDataException jde) {
							LOG.error("hgetAll {}({}) failed!, because {}", Arrays.toString(key), new String(key), jde);
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
							final ScanResult<Map.Entry<byte[], byte[]>> hscan =
									client.hscan(key, cursorAsBytes, scanParams);
							for (Map.Entry<byte[], byte[]> entry : hscan.getResult()) {
								RowData r = recordMapper.generate(key, entry.getKey(), entry.getValue());
								res.add(r);
							}
							cursorAsBytes = hscan.getCursorAsBytes();
						} while (!Arrays.equals(cursorAsBytes, SCAN_POINTER_START_BINARY));
					} catch (JedisDataException jde) {
						if (!ignoreError) {
							throw jde;
						}
						LOG.error("hgetAll {}({}) failed!, because {}", Arrays.toString(key), new String(key), jde);
					}
				}
				return res;
			};
		}
	}

	private RowData convertStringTupleToRowData(byte[] k, byte[] f, byte[] v) {
		return GenericRowData.ofKind(INSERT, fromBytes(k), fromBytes(f), fromBytes(v));
	}

	private RowData convertBytesTupleToRowData(byte[] k, byte[] f, byte[] v) {
		return GenericRowData.ofKind(INSERT, k, f, v);
	}

	private RowData convertStringMapToRowData(byte[] k, Map<byte[], byte[]> m) {
		final HashMap<Object, Object> map = new HashMap<>();
		m.forEach((f1, f2) -> map.put(StringData.fromBytes(f1), StringData.fromBytes(f2)));
		return GenericRowData.ofKind(
				INSERT, StringData.fromBytes(k), new GenericMapData(map));
	}

	private RowData convertBytesMapToRowData(byte[] k, Map<byte[], byte[]> m) {
		return GenericRowData.ofKind(INSERT, k, new GenericMapData(m));
	}

	@Override
	public List<RowData> apply(List<byte[]> keys, RedisClientProxy client, boolean ignoreError) {
		return delegate.apply(keys, client, ignoreError);
	}

	@FunctionalInterface
	public interface RecordGenerator<I, O> extends Function {
		O generate(I key, I field, I value);
	}

	@FunctionalInterface
	public interface MapRecordGenerator<I, O> extends Function {
		O generate(I key, Map<I, I> map);
	}
}
