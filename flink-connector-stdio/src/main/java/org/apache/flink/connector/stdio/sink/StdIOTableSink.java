package org.apache.flink.connector.stdio.sink;

import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

/**
 * StdIOTableSink for standard output stream.
 */
public class StdIOTableSink implements DynamicTableSink {

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return requestedMode;
	}

	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		return SinkFunctionProvider.of(new PrintSinkFunction<>());
	}

	@Override
	public DynamicTableSink copy() {
		return new StdIOTableSink();
	}

	@Override
	public String asSummaryString() {
		return "StdIOTableSink";
	}
}
