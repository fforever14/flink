package org.apache.flink.connector.redis.table;

import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;

public abstract class RedisDataResolver implements Serializable {

	private static final long serialVersionUID = 1L;

	protected RedisDataResolveConf conf;
	protected SerializationSchema<RowData> serializationSchema;

	public RedisDataResolver(RedisDataResolveConf conf, SerializationSchema<RowData> serializationSchema) {
		this.conf = conf;
		this.serializationSchema = serializationSchema;
	}

	public abstract void invoke(RowData value, RedisCommands<String, String> redisClient, SinkFunction.Context context);
}
