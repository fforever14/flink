package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.OracleRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Oracle Dialect
 */
public class OracleDialect extends AbstractDialect {

	public static final Integer ORACLE_MAX_PRECISION = 38;
	public static final Integer ORACLE_MIN_PRECISION = 1;

	//https://docs.oracle.com/database/nosql-12.1.4.3/SQLForNoSQL/timestamp.html
	public static final Integer ORACLE_TIMESTAMP_MAX_PRECISIONT = 9;
	public static final Integer ORACLE_TIMESTAMP_MIN_PRECISIONT = 0;

	@Override
	public int maxDecimalPrecision() {
		return ORACLE_MAX_PRECISION;
	}

	@Override
	public int minDecimalPrecision() {
		return ORACLE_MIN_PRECISION;
	}

	@Override
	public int maxTimestampPrecision() {
		return ORACLE_TIMESTAMP_MAX_PRECISIONT;
	}

	@Override
	public int minTimestampPrecision() {
		return ORACLE_TIMESTAMP_MIN_PRECISIONT;
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return identifier;
	}

	@Override
	public List<LogicalTypeRoot> unsupportedTypes() {
		return Arrays.asList(
			LogicalTypeRoot.ARRAY,
			LogicalTypeRoot.INTERVAL_DAY_TIME,
			LogicalTypeRoot.INTERVAL_YEAR_MONTH,
			LogicalTypeRoot.VARBINARY,
			LogicalTypeRoot.UNRESOLVED,
			LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
			LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
			LogicalTypeRoot.SYMBOL,
			LogicalTypeRoot.MAP,
			LogicalTypeRoot.MULTISET,
			LogicalTypeRoot.RAW,
			LogicalTypeRoot.ROW,
			LogicalTypeRoot.SMALLINT,
			LogicalTypeRoot.STRUCTURED_TYPE
		);
	}

	@Override
	public String dialectName() {
		return "Oracle";
	}

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:oracle:thin");
	}

	@Override
	public JdbcRowConverter getRowConverter(RowType rowType) {
		return new OracleRowConverter(rowType);
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of("oracle.jdbc.OracleDriver");
	}
}
