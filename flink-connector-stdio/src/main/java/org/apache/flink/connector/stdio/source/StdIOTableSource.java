package org.apache.flink.connector.stdio.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

/**
 * StdIOTableSource for standard input stream.
 */
public class StdIOTableSource implements ScanTableSource {

	private final DataType producedDataType;
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

	public StdIOTableSource(DataType producedDataType, DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
		this.producedDataType = producedDataType;
		this.decodingFormat = decodingFormat;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		DeserializationSchema<RowData> deserializationSchema = decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);
		return SourceFunctionProvider.of(new StdIOSourceFunction<>(deserializationSchema), false);
	}

	@Override
	public DynamicTableSource copy() {
		return new StdIOTableSource(producedDataType, decodingFormat);
	}

	@Override
	public String asSummaryString() {
		return "StdIOTableSource";
	}
}
