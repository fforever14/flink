package org.apache.flink.connector.redis.utils;

public enum RedisType {
	STRING("string"), HASH("hash");

	private String typeName;

	RedisType(String typeName) {
		this.typeName = typeName;
	}

	@Override
	public String toString() {
		return typeName;
	}

	public static RedisType of(String typeName) {
		for (RedisType redisType : RedisType.values()) {
			if (redisType.typeName.equals(typeName)) {
				return redisType;
			}
		}
		throw new IllegalArgumentException(String.format("unknown typeName: %s", typeName));
	}
}
