package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;

@Internal
public class RedisValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_REDIS = "redis";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
	}
}
