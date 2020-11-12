package org.apache.flink.table.descriptors;

import java.util.Map;

import static org.apache.flink.table.descriptors.RedisValidator.CONNECTOR_TYPE_VALUE_REDIS;

public class Redis extends ConnectorDescriptor {

	public Redis() {
		super(CONNECTOR_TYPE_VALUE_REDIS, 1, true);
	}

	@Override
	protected Map<String, String> toConnectorProperties() {
		return null;
	}
}
