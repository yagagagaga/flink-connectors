package org.apache.flink.connector.stdio.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * StdIOTableSink for standard output stream.
 */
public class StdIOTableSink implements DynamicTableSink {

	private final DataType producedDataType;
	private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;

	public StdIOTableSink(DataType producedDataType, EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		this.producedDataType = producedDataType;
		this.encodingFormat = encodingFormat;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return requestedMode;
	}

	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		SerializationSchema<RowData> serializationSchema = encodingFormat.createRuntimeEncoder(context, producedDataType);
		return SinkFunctionProvider.of(new StdIOSinkFunction(serializationSchema));
	}

	@Override
	public DynamicTableSink copy() {
		return new StdIOTableSink(producedDataType, encodingFormat);
	}

	@Override
	public String asSummaryString() {
		return "StdIOTableSink";
	}
}
