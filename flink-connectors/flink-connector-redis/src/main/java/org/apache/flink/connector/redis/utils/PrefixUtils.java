package org.apache.flink.connector.redis.utils;

import org.apache.commons.lang3.StringUtils;

public class PrefixUtils {
	public static String buildKey(String prefix, String key) {
		if (StringUtils.isBlank(prefix)) {
			return key;
		} else {
			return prefix + key;
		}
	}
}
