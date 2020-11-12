package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.dialect.OracleDialect;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.URL;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * oracle catalog.
 * Donald @author
 */
public class OracleCatalog extends AbstractJdbcCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(OracleCatalog.class);

	public OracleCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
		super(catalogName, defaultDatabase, username, pwd, baseUrl);
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		List<String> oracleDbs = new ArrayList<>();
		try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
			oracleDbs.add(username.toUpperCase());
			return oracleDbs;
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed listing database in catalog %s", getName()), e);
		}
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws CatalogException {
		if (listDatabases().contains(databaseName.toUpperCase())) {
			return new CatalogDatabaseImpl(Collections.emptyMap(), null);
		} else {
			// 这行应该永远不会执行
			throw new CatalogException("This is a bug, please contact developer!");
		}
	}

	@Override
	public List<String> listTables(String databaseName) throws CatalogException {
		List<String> tableLst = new ArrayList<>();
		try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
			PreparedStatement ps = conn.prepareStatement("SELECT TABLE_NAME FROM USER_TABLES");
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				tableLst.add(rs.getString(1));
			}
			return tableLst;
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed listing table in catalog %s", getName()), e);
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}
		try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {
			PreparedStatement ps = conn.prepareStatement(
				"SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE FROM USER_TAB_COLS" +
					" WHERE TABLE_NAME = '" + tablePath.getObjectName().toUpperCase() + "'" +
					" ORDER BY COLUMN_ID"
			);

			ResultSet rs = ps.executeQuery();
			List<String> cols = new ArrayList<>();
			List<DataType> dataTypes = new ArrayList<>();

			while (rs.next()) {
				String columnName = rs.getString(1);
				String dataType = rs.getString(2);
				Integer dataLength = rs.getInt(3);
				Integer precision = rs.getInt(4);
				Integer scale = rs.getInt(5);
				cols.add(columnName);
				dataTypes.add(fromJDBCType(dataType, dataLength, precision, scale));
			}

			TableSchema.Builder tableBuilder = new TableSchema.Builder();
			for (int i = 0, length = cols.size(); i < length; i++) {
				tableBuilder.field(cols.get(i), dataTypes.get(i));
			}

			PreparedStatement stat = conn.prepareStatement(
				"select a.constraint_name,  a.column_name " +
					" from user_cons_columns a, user_constraints b " +
					" where a.constraint_name = b.constraint_name " +
					" and b.constraint_type = 'P' and a.table_name = '" + tablePath.getObjectName().toUpperCase() + "'"
			);
			ResultSet pkeyRs = stat.executeQuery();
			if (pkeyRs.next()) {
				String[] pkeyArr = new String[1];
				pkeyArr[0] = pkeyRs.getString(2);
				tableBuilder.primaryKey(pkeyRs.getString(1), pkeyArr);
			}

			TableSchema tableSchema = tableBuilder.build();
			Map<String, String> props = new HashMap<>();
			props.put(CONNECTOR.key(), IDENTIFIER);
			props.put(URL.key(), defaultUrl);
			props.put(TABLE_NAME.key(), tablePath.getFullName());
			props.put(USERNAME.key(), username);
			props.put(PASSWORD.key(), pwd);

			return new CatalogTableImpl(tableSchema, props, "");
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed getting table %s", tablePath.getFullName()), e);
		}
	}

	public static final String ORACLE_VARCHAR = "VARCHAR";
	public static final String ORACLE_NVARCHAR = "NVARCHAR";
	public static final String ORACLE_VARCHAR2 = "VARCHAR2";
	public static final String ORACLE_NVARCHAR2 = "NVARCHAR2";
	public static final String ORACLE_INTEGER  = "INTEGER";
	public static final String ORACLE_DATE = "DATE";
	public static final String ORACLE_TIMESTAMP = "TIMESTAMP";
	public static final String ORACLE_CHAR = "CHAR";
	public static final String ORACLE_FLOAT = "FLOAT";
	public static final String ORACLE_LONG = "LONG";
	public static final String ORACLE_NUMBER = "NUMBER";
	public static final String ORACLE_DOUBLE = "DOUBLE";
	public static final String ORACLE_TIMESTAMP_WITH_TIME_ZONE = "TIMESTAMP WITH TIME ZONE";
	public static final String ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = "TIMESTAMP WITH LOCAL TIME ZONE";
	public static final String ORACLE_TINYINT = "TINYINT";

	private DataType fromJDBCType(String dataType, Integer dataLength, Integer precision, Integer scale) {
		if (dataType.startsWith(ORACLE_TIMESTAMP)) {
			switch (dataType) {
				case ORACLE_TIMESTAMP_WITH_TIME_ZONE:
					return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
				case ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
					return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
				default:
					String[] arr = dataType.split("\\(");
					if (arr.length == 2) {
						return DataTypes.TIMESTAMP(Integer.parseInt(arr[1].replace("\\)", "")));
					} else {
						throw new UnsupportedOperationException(
							String.format("Doesn't support Postgres type '%s' yet", dataType));
					}
			}
		} else {
			switch (dataType) {
				case ORACLE_VARCHAR:
					return DataTypes.VARCHAR(dataLength);
				case ORACLE_NVARCHAR:
				case ORACLE_VARCHAR2:
				case ORACLE_NVARCHAR2:
					return DataTypes.STRING();
				case ORACLE_INTEGER:
					return DataTypes.INT();
				case ORACLE_TINYINT:
					return DataTypes.TINYINT();
				case ORACLE_CHAR:
					return DataTypes.CHAR(dataLength);
				case ORACLE_FLOAT:
					return DataTypes.FLOAT();
				case ORACLE_LONG:
					return DataTypes.BIGINT();
				case ORACLE_NUMBER:
					if (precision == 0) {
						precision = OracleDialect.ORACLE_MAX_PRECISION;
					}
					return DataTypes.DECIMAL(precision, scale);
				case ORACLE_DOUBLE:
					return DataTypes.DOUBLE();
				case ORACLE_DATE:
					return DataTypes.TIMESTAMP(0);
				default:
					throw new UnsupportedOperationException(
						String.format("Doesn't support Oracle type '%s' yet", dataType));
			}
		}
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		return listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
	}
}
