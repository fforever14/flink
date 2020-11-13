package org.apache.flink.connector.redis.table;

import org.apache.flink.connector.redis.utils.RedisType;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;

public class RedisDataResolveConf implements Serializable {

	private static final long serialVersionUID = 1L;

	private RedisType redisType;
	private String prefix;
	private Integer ttl;
	private int[] primaryKeyIndexArr;
	private DataType[] primaryKeyDataTypeArr;


	private RedisDataResolveConf(RedisType redisType, String prefix, Integer ttl, int[] primaryKeyIndexArr, DataType[] primaryKeyDataTypeArr) {
		this.redisType = redisType;
		this.prefix = prefix;
		this.ttl = ttl;
		this.primaryKeyIndexArr = primaryKeyIndexArr;
		this.primaryKeyDataTypeArr = primaryKeyDataTypeArr;
	}

	public static Builder builder() {
		return new Builder();
	}

	public RedisType getRedisType() {
		return redisType;
	}

	public String getPrefix() {
		return prefix;
	}

	public Integer getTtl() {
		return ttl;
	}

	public int[] getPrimaryKeyIndexArr() {
		return primaryKeyIndexArr;
	}

	public DataType[] getPrimaryKeyDataTypeArr() {
		return primaryKeyDataTypeArr;
	}

	public static class Builder {
		private RedisType redisType;
		private String prefix;
		private Integer ttl;
		private int[] primaryKeyIndexArr;
		private DataType[] primaryKeyDataTypeArr;

		public Builder setRedisType(RedisType redisType) {
			this.redisType = redisType;
			return this;
		}

		public Builder setPrefix(String prefix) {
			this.prefix = prefix;
			return this;
		}

		public Builder setTtl(Integer ttl) {
			this.ttl = ttl;
			return this;
		}

		public Builder setPrimaryKeyIndexArr(int[] primaryKeyIndexArr) {
			this.primaryKeyIndexArr = primaryKeyIndexArr;
			return this;
		}

		public Builder setPrimaryKeyDataTypeArr(DataType[] primaryKeyDataTypeArr) {
			this.primaryKeyDataTypeArr = primaryKeyDataTypeArr;
			return this;
		}

		public RedisDataResolveConf build() {
			return new RedisDataResolveConf(redisType, prefix, ttl, primaryKeyIndexArr, primaryKeyDataTypeArr);
		}
	}
}
