package org.apache.flink.connector.stdio.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.StringUtils;

import java.util.Scanner;

/**
 * StdIOSourceFunction for standard input stream.
 */
public class StdIOSourceFunction implements SourceFunction<RowData> {

	private final DeserializationSchema<RowData> deserializationSchema;
	private volatile boolean running = true;

	public StdIOSourceFunction(DeserializationSchema<RowData> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public void run(SourceContext<RowData> sourceContext) throws Exception {
		System.out.println("======== ready to receive data, type `exit` or `bye` to end stream ========");
		try (Scanner scanner = new Scanner(System.in)) {
			while (running && scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if (StringUtils.isEmpty(line)) {
					continue;
				}
				if ("exit".equalsIgnoreCase(line.trim()) || "bye".equalsIgnoreCase(line.trim())) {
					break;
				}
				RowData deserialize = deserializationSchema.deserialize(line.getBytes());
				sourceContext.collect(deserialize);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
