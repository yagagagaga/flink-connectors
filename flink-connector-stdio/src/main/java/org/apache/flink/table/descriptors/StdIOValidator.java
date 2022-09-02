package org.apache.flink.table.descriptors;

/**
 * The validator for StdIO.
 */
public class StdIOValidator extends ConnectorDescriptorValidator {

	public static final String IDENTIFIER = "std.io";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, IDENTIFIER, false);
	}
}
