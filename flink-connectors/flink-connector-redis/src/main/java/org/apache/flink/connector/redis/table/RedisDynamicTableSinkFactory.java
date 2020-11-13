package org.apache.flink.connector.redis.table;

import io.lettuce.core.RedisURI;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.utils.RedisType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

public class RedisDynamicTableSinkFactory implements DynamicTableSinkFactory {


	public static final ConfigOption<String> URL = ConfigOptions
		.key("url")
		.stringType()
		.noDefaultValue()
		.withDescription("lettuce redis uri syntax.");

	public static final ConfigOption<String> REDIS_TYPE = ConfigOptions
		.key("redis-type")
		.stringType()
		.defaultValue("string")
		.withDescription("redis data type. support string, hashmap.");

	public static final ConfigOption<Integer> TTL = ConfigOptions
		.key("ttl")
		.intType()
		.noDefaultValue()
		.withDescription("key ttl in seconds.");

	public static final ConfigOption<String> KEY_PREFIX = ConfigOptions
		.key("key-prefix")
		.stringType()
		.noDefaultValue()
		.withDescription("key prefix, will add to every key");


	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		final EncodingFormat<SerializationSchema<RowData>> encodingFormat = helper.discoverEncodingFormat(
			SerializationFormatFactory.class,
			FactoryUtil.FORMAT);
		helper.validate();
		final ReadableConfig options = helper.getOptions();
		String url = options.get(URL);
		RedisURI redisURI = RedisURI.create(url);
		RedisType redisType = RedisType.of(options.get(REDIS_TYPE));
		TableSchema ts = context.getCatalogTable().getSchema();
		DataType rowDataType = ts.toPhysicalRowDataType();
		int[] primaryKeyIndexArr = TableSchemaUtils.getPrimaryKeyIndices(ts);
		DataType[] primaryKeyDataTypeArr = new DataType[0];
		if (primaryKeyIndexArr.length > 0) {
			primaryKeyDataTypeArr = new DataType[primaryKeyIndexArr.length];
			for (int i = 0; i < primaryKeyIndexArr.length; i++) {
				primaryKeyDataTypeArr[i] = ts.getFieldDataType(primaryKeyIndexArr[i]).get();
			}
		}
		validatePrimaryKey(redisType, ts);
		return new RedisDynamicTableSink(
			redisURI,
			redisType,
			options.getOptional(TTL),
			options.getOptional(KEY_PREFIX).orElse(""),
			rowDataType,
			primaryKeyIndexArr,
			primaryKeyDataTypeArr,
			encodingFormat
		);
	}

	private void validatePrimaryKey(RedisType redisType, TableSchema ts) {
		if (RedisType.STRING == redisType) {
			if (!ts.getPrimaryKey().isPresent()) {
				throw new IllegalArgumentException(
					String.format("%s type must declare 1 primary key", RedisType.STRING.toString())
				);
			}
			ts.getPrimaryKey().ifPresent(constraint -> {
				if (constraint.getColumns().size() != 1) {
					throw new IllegalArgumentException(
						String.format("%s type only support 1 primary key", RedisType.STRING.toString())
					);
				}
			});
		}
		if (RedisType.HASH == redisType) {
			if (!ts.getPrimaryKey().isPresent()) {
				throw new IllegalArgumentException(
					String.format("%s type must declare 2 primary key", RedisType.HASH.toString())
				);
			}
			ts.getPrimaryKey().ifPresent(constraint -> {
				if (constraint.getColumns().size() != 2) {
					throw new IllegalArgumentException(
						String.format("%s type only support 2 primary key", RedisType.HASH.toString())
					);
				}
			});
		}
	}

	@Override
	public String factoryIdentifier() {
		return "redis";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> rst = new HashSet<>();
		rst.add(URL);
		rst.add(REDIS_TYPE);
		rst.add(FactoryUtil.FORMAT);
		return rst;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> rst = new HashSet<>();
		rst.add(TTL);
		rst.add(KEY_PREFIX);
		return rst;
	}

}
