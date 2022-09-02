package org.apache.flink.table.descriptors;

import java.util.Map;

import static org.apache.flink.table.descriptors.StdIOValidator.IDENTIFIER;

/**
 * Connector descriptor for StdIO.
 */
@Deprecated
public class StdIO extends ConnectorDescriptor {
	private final DescriptorProperties properties = new DescriptorProperties();

	public StdIO() {
		super(IDENTIFIER, 1, true);
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		return properties.asMap();
	}
}
