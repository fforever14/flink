package org.apache.flink.connector.redis.table;

import io.lettuce.core.api.sync.RedisCommands;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.redis.utils.PrefixUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class StringResolver extends RedisDataResolver<RowData> {

	private RowData.FieldGetter keyGetter;

	public StringResolver(RedisDataResolveConf conf, SerializationSchema<RowData> serializationSchema) {
		super(conf, serializationSchema);
		int primaryKeyIndex = conf.getPrimaryKeyIndexArr()[0];
		DataType primaryKeyDataType = conf.getPrimaryKeyDataTypeArr()[0];
		keyGetter = RowData.createFieldGetter(primaryKeyDataType.getLogicalType(), primaryKeyIndex);
	}

	private RedisData resolve(RowData value) {
		RedisData rst = new RedisData();
		rst.setRedisType(conf.getRedisType());
		rst.setValue(new String(serializationSchema.serialize(value)));
		rst.setKey(PrefixUtils.buildKey(conf.getPrefix(), keyGetter.getFieldOrNull(value).toString()));
		return rst;
	}

	@Override
	public void invoke(RowData value, RedisCommands<String, String> commands, SinkFunction.Context context) {
		RedisData data = resolve(value);
		commands.set(data.getKey(), data.getValue());
		if (conf.getTtl() > 0) {
			commands.expire(data.getKey(), conf.getTtl());
		}
	}
}
