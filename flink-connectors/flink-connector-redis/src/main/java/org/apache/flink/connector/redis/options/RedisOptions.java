package org.apache.flink.connector.redis.options;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class RedisOptions {

	private final String redisIp;
	private final Integer redisPort;
	private final Integer databaseIndex;
	private final String redisType;

	public RedisOptions(String redisIp, Integer redisPort, Integer databaseIndex, String redisType) {
		this.redisIp = redisIp;
		this.redisPort = redisPort;
		this.databaseIndex = databaseIndex;
		this.redisType = redisType;
	}

	public String getRedisIp() {
		return redisIp;
	}

	public Integer getRedisPort() {
		return redisPort;
	}

	public Integer getDatabaseIndex() {
		return databaseIndex;
	}

	public String getRedisType() {
		return redisType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RedisOptions that = (RedisOptions) o;
		return redisIp.equals(that.redisIp) &&
			redisPort.equals(that.redisPort) &&
			databaseIndex.equals(that.databaseIndex) &&
			redisType.equals(that.redisType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(redisIp, redisPort, databaseIndex, redisType);
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private String redisIp;
		private String redisPort;
		private String databaseIndex;
		private String redisType;

		public Builder setRedisIp(String redisIp) {
			checkNotNull(redisIp);
			this.redisIp = redisIp;
			return this;
		}

		public Builder setRedisPort(String redisPort) {
			checkNotNull(redisPort);
			this.redisPort = redisPort;
			return this;
		}

		public Builder setDatabaseIndex(String databaseIndex) {
			checkNotNull(databaseIndex);
			this.databaseIndex = databaseIndex;
			return this;
		}

		public Builder setRedisType(String redisType) {
			checkNotNull(redisType);
			this.redisType = redisType;
			return this;
		}

		public RedisOptions build() {
			checkNotNull(redisIp);
			checkNotNull(redisPort);
			checkNotNull(databaseIndex);
			if (StringUtils.isBlank(redisType)) {
				redisType = "standalone";
			}
			return new RedisOptions(
				redisIp,
				Integer.parseInt(redisPort),
				Integer.parseInt(databaseIndex),
				redisType
			);
		}
	}
}
