package org.apache.flink.connector.redis.table;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class RedisSinkFunction<T> extends RichSinkFunction<T> {

	protected RedisURI redisURI;
	protected RedisDataResolver<T> resolver;
	protected RedisClient redisClient;
	protected RedisCommands<String, String> commands;


	public RedisSinkFunction(RedisURI redisURI, RedisDataResolver<T> resolver) {
		this.redisURI = redisURI;
		this.resolver = resolver;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		redisClient = RedisClient.create(redisURI);
		commands = redisClient.connect().sync();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (redisClient != null) {
			redisClient.shutdown();
		}
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		resolver.invoke(value, commands, context);
	}
}
