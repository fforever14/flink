package org.apache.flink.connector.redis.table;

import io.lettuce.core.RedisURI;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.redis.utils.RedisType;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Optional;

public class RedisDynamicTableSink implements DynamicTableSink {

	private RedisURI redisURI;
	private RedisType redisType;
	private String keyPrefix;
	private Optional<Integer> ttl;
	private DataType rowDataType;
	private int[] primaryKeyIndexArr;
	private DataType[] primaryKeyDataTypeArr;
	private EncodingFormat<SerializationSchema<RowData>> encodingFormat;

	public RedisDynamicTableSink(RedisURI redisURI,
								 RedisType redisType,
								 Optional<Integer> ttl,
								 String keyPrefix,
								 DataType rowDataType,
								 int[] primaryKeyIndexArr,
								 DataType[] primaryKeyDataTypeArr,
								 EncodingFormat<SerializationSchema<RowData>> encodingFormat) {
		this.redisURI = redisURI;
		this.redisType = redisType;
		this.ttl = ttl;
		this.rowDataType = rowDataType;
		this.primaryKeyIndexArr = primaryKeyIndexArr;
		this.primaryKeyDataTypeArr = primaryKeyDataTypeArr;
		this.keyPrefix = keyPrefix;
		this.encodingFormat = encodingFormat;
	}


	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		return encodingFormat.getChangelogMode();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		SerializationSchema<RowData> ss = encodingFormat.createRuntimeEncoder(context, rowDataType);
		RedisDataResolveConf conf = RedisDataResolveConf.builder()
			.setRedisType(redisType)
			.setTtl(ttl.orElse(-1))
			.setPrefix(keyPrefix)
			.setPrimaryKeyIndexArr(primaryKeyIndexArr)
			.setPrimaryKeyDataTypeArr(primaryKeyDataTypeArr)
			.build();
		RedisDataResolver resolver = createResolver(conf, ss);
		RedisSinkFunction function = new RedisSinkFunction(redisURI, resolver);
		return SinkFunctionProvider.of(function);
	}

	private RedisDataResolver createResolver(RedisDataResolveConf conf, SerializationSchema<RowData> ss) {
		if (conf.getRedisType() == RedisType.STRING) {
			return new StringResolver(conf, ss);
		}
		if (conf.getRedisType() == RedisType.HASH) {
			return new HashMapResolver(conf, ss);
		}
		throw new IllegalArgumentException("resolver is null");
	}

	@Override
	public DynamicTableSink copy() {
		return new RedisDynamicTableSink(redisURI, redisType, ttl, keyPrefix, rowDataType, primaryKeyIndexArr, primaryKeyDataTypeArr, encodingFormat);
	}

	@Override
	public String asSummaryString() {
		return "Redis Table Sink";
	}
}
