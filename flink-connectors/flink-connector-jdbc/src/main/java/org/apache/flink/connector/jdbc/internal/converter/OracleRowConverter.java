package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.table.types.logical.RowType;

/**
 * Runtime converter that responsible to convert between JDBC object and Flink internal object for Oracle.
 */
public class OracleRowConverter extends AbstractJdbcRowConverter {

	private static final long serialVersionUID = 1L;

	public OracleRowConverter(RowType rowType) {
		super(rowType);
	}

	@Override
	public String converterName() {
		return "Oracle";
	}
}
