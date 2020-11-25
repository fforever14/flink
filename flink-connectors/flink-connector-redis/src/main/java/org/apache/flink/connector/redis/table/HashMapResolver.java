package org.apache.flink.connector.redis.table;

import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class HashMapResolver extends RedisDataResolver {

	private RowData.FieldGetter keyGetter;
	private RowData.FieldGetter hashKeyGetter;

	public HashMapResolver(RedisDataResolveConf conf, SerializationSchema<RowData> serializationSchema) {
		super(conf, serializationSchema);
		keyGetter = RowData.createFieldGetter(
			conf.getPrimaryKeyDataTypeArr()[0].getLogicalType(),
			conf.getPrimaryKeyIndexArr()[0]
		);
		hashKeyGetter = RowData.createFieldGetter(
			conf.getPrimaryKeyDataTypeArr()[1].getLogicalType(),
			conf.getPrimaryKeyIndexArr()[1]
		);
	}

	private RedisData resolve(RowData value) {
		RedisData rst = new RedisData();
		rst.setRedisType(conf.getRedisType());
		rst.setValue(new String(serializationSchema.serialize(value)));
		rst.setKey(keyGetter.getFieldOrNull(value).toString());
		rst.setHashKey(hashKeyGetter.getFieldOrNull(value).toString());
		return rst;
	}

	@Override
	public void invoke(RowData value, RedisCommands<String, String> commands, SinkFunction.Context context) {
		RowKind rowKind = value.getRowKind();
		RedisData data = resolve(value);
		if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
			commands.hset(data.getKey(), data.getHashKey(), data.getValue());
			if (conf.getTtl() > 0) {
				commands.expire(data.getKey(), conf.getTtl());
			}
		}
		if (RowKind.DELETE.equals(rowKind)) {
			commands.hdel(data.getKey(), data.getHashKey());
		}
	}
}
