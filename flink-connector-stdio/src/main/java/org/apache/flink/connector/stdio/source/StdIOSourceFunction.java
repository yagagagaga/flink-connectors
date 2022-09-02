package org.apache.flink.connector.stdio.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.commons.lang3.StringUtils;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.PrintWriter;

/**
 * StdIOSourceFunction for standard input stream.
 */
public class StdIOSourceFunction<T> implements SourceFunction<T> {

	private final DeserializationSchema<T> deserializationSchema;
	private volatile boolean running = true;

	public StdIOSourceFunction(DeserializationSchema<T> deserializationSchema) {
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		try (Terminal terminal = TerminalBuilder.terminal();
			 PrintWriter printWriter = terminal.writer()
		) {
			printWriter.println("======== ready to receive data, type `exit` or `bye` to end stream ========");
			printWriter.flush();
			LineReader lineReader = LineReaderBuilder.builder().terminal(terminal).build();

			String line;
			while (running && (line = lineReader.readLine("Type here> ")) != null) {
				if (StringUtils.isEmpty(line)) {
					continue;
				}
				if ("exit".equalsIgnoreCase(line.trim()) || "bye".equalsIgnoreCase(line.trim())) {
					break;
				}
				T deserialize = deserializationSchema.deserialize(line.getBytes());
				sourceContext.collect(deserialize);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
