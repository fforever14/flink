package org.apache.flink.connector.redis.table;

import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

public abstract class RedisDataResolver<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	protected RedisDataResolveConf conf;
	protected SerializationSchema<T> serializationSchema;

	public RedisDataResolver(RedisDataResolveConf conf, SerializationSchema<T> serializationSchema) {
		this.conf = conf;
		this.serializationSchema = serializationSchema;
	}

	public abstract void invoke(T value, RedisCommands<String, String> redisClient, SinkFunction.Context context);
}
