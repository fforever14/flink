package org.apache.flink.connector.redis.table;

import org.apache.flink.connector.redis.utils.RedisType;

public class RedisData {

	private RedisType redisType;
	private String key;
	private String hashKey;
	private Long ts;
	private String value;

	public RedisType getRedisType() {
		return redisType;
	}

	public String getKey() {
		return key;
	}

	public String getHashKey() {
		return hashKey;
	}

	public Long getTs() {
		return ts;
	}

	public String getValue() {
		return value;
	}

	public void setRedisType(RedisType redisType) {
		this.redisType = redisType;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public void setHashKey(String hashKey) {
		this.hashKey = hashKey;
	}

	public void setTs(Long ts) {
		this.ts = ts;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "RedisData{" +
			"redisType=" + redisType +
			", key='" + key + '\'' +
			", hashKey='" + hashKey + '\'' +
			", ts=" + ts +
			", value='" + value + '\'' +
			'}';
	}
}
