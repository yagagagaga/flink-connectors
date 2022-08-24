package org.apache.flink.connector.stdio.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

import java.nio.charset.Charset;

/**
 * StdIOSinkFunction for standard output stream.
 */
public class StdIOSinkFunction implements SinkFunction<RowData> {

	private final SerializationSchema<RowData> serializationSchema;

	public StdIOSinkFunction(SerializationSchema<RowData> serializationSchema) {
		this.serializationSchema = serializationSchema;
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		byte[] serialize = serializationSchema.serialize(value);
		System.out.println(String.format(
				"%s(%s)",
				value.getRowKind().shortString(),
				new String(serialize, Charset.defaultCharset())));
	}
}
