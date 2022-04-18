package org.apache.flink.connector.redis;

import org.apache.flink.table.descriptors.Descriptor;
import org.apache.flink.table.descriptors.DescriptorTestBase;
import org.apache.flink.table.descriptors.DescriptorValidator;
import org.apache.flink.table.descriptors.Redis;
import org.apache.flink.table.descriptors.RedisValidator;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test case for {@link Redis} descriptor.
 */
public class RedisDescriptorTest extends DescriptorTestBase {
	@Override
	protected List<Descriptor> descriptors() {
		return Arrays.asList(new Redis(), new Redis());
	}

	@Override
	protected List<Map<String, String>> properties() {
		Map<String, String> prop0 = new HashMap<>();
		Map<String, String> prop1 = new HashMap<>();
		return Arrays.asList(prop0, prop1);
	}

	@Override
	protected DescriptorValidator validator() {
		return new RedisValidator();
	}

	@Test
	public void testRequiredFields() {

	}
}
