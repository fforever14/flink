package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.IDENTIFIER;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.PASSWORD;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.URL;
import static org.apache.flink.connector.jdbc.table.JdbcDynamicTableFactory.USERNAME;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Catalog for MySQL.
 */
public class MySQLCatalog extends AbstractJdbcCatalog {

	private static final Logger LOG = LoggerFactory.getLogger(MySQLCatalog.class);

	public MySQLCatalog(String catalogName, String defaultDatabase, String username, String pwd, String baseUrl) {
		super(catalogName, defaultDatabase, username, pwd, baseUrl);
	}

	@Override
	public List<String> listDatabases() throws CatalogException {
		List<String> databases = new ArrayList<>();

		try (Connection conn = DriverManager.getConnection(defaultUrl, username, pwd)) {

			PreparedStatement ps = conn.prepareStatement("show databases;");

			ResultSet rs = ps.executeQuery();

			while (rs.next()) {
				String dbName = rs.getString(1);
				if (!StringUtils.isNullOrWhitespaceOnly(dbName)) {
					databases.add(rs.getString(1));
				}
			}

			return databases;
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed listing database in catalog %s", getName()), e);
		}
	}

	@Override
	public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (listDatabases().contains(databaseName)) {
			return new CatalogDatabaseImpl(Collections.emptyMap(), null);
		} else {
			throw new DatabaseNotExistException(getName(), databaseName);
		}
	}

	@Override
	public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
		if (!databaseExists(databaseName)) {
			throw new DatabaseNotExistException(getName(), databaseName);
		}

		// get all schemas
		try (Connection conn = DriverManager.getConnection(baseUrl + databaseName, username, pwd)) {
			PreparedStatement ps = conn.prepareStatement(
				"SELECT table_name " +
					"FROM information_schema.tables " +
					"WHERE table_type='BASE TABLE' " +
					"AND table_schema='" + databaseName + "' " +
					"order by table_name;");

			ResultSet rs = ps.executeQuery();

			List<String> tables = new ArrayList<>();

			while (rs.next()) {
				String tableName = rs.getString(1);
				if (!StringUtils.isNullOrWhitespaceOnly(tableName)) {
					tables.add(tableName);
				}
			}
			return tables;
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed listing database in catalog %s", getName()), e);
		}
	}

	@Override
	public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
		if (!tableExists(tablePath)) {
			throw new TableNotExistException(getName(), tablePath);
		}

		String dbUrl = baseUrl + tablePath.getDatabaseName();
		try (Connection conn = DriverManager.getConnection(dbUrl, username, pwd)) {
			PreparedStatement ps = conn.prepareStatement(
				"select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION" +
					" from information_schema.COLUMNS" +
					" WHERE TABLE_NAME = '" + tablePath.getObjectName() + "'" +
					" AND TABLE_SCHEMA = '" + tablePath.getDatabaseName() + "'" +
					" ORDER BY ORDINAL_POSITION"
			);

			ResultSet rs = ps.executeQuery();
			List<String> cols = new ArrayList<>();
			List<DataType> dataTypes = new ArrayList<>();

			PreparedStatement stat = conn.prepareStatement(
				"select kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME from information_schema.KEY_COLUMN_USAGE kcu " +
					"inner join information_schema.TABLE_CONSTRAINTS tc " +
					"using (constraint_name,table_schema,table_name) " +
					"where tc.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
					"and kcu.TABLE_SCHEMA = '" + tablePath.getDatabaseName() + "' " +
					"and kcu.TABLE_NAME = '" + tablePath.getObjectName() + "' " +
					"order by kcu.TABLE_NAME, kcu.ORDINAL_POSITION"
			);
			ResultSet pkeyRs = stat.executeQuery();
			String pkeyName = null;
			List<String> pkeyList = new ArrayList<>();
			while (pkeyRs.next()) {
				if (pkeyName == null) {
					pkeyName = pkeyRs.getString(1);
				}
				pkeyList.add(pkeyRs.getString(2));
			}

			TableSchema.Builder tableBuilder = new TableSchema.Builder();

			while (rs.next()) {
				String columnName = rs.getString(1);
				String dataType = rs.getString(2);
				Long charLength = rs.getLong(3);
				Long numPrecision = rs.getLong(4);
				Long numScale = rs.getLong(5);
				Long datetimePrecision = rs.getLong(6);
				cols.add(columnName);
				dataTypes.add(fromJDBCType(dataType, charLength, numPrecision, numScale, datetimePrecision, pkeyList.contains(columnName)));
			}

			for (int i = 0, length = cols.size(); i < length; i++) {
				tableBuilder.field(cols.get(i), dataTypes.get(i));
			}


			if (pkeyName != null) {
				tableBuilder.primaryKey(pkeyName, pkeyList.toArray(new String[pkeyList.size()]));
			}

			TableSchema tableSchema = tableBuilder.build();

			Map<String, String> props = new HashMap<>();
			props.put(CONNECTOR.key(), IDENTIFIER);
			props.put(URL.key(), dbUrl);
			props.put(TABLE_NAME.key(), tablePath.getObjectName());
			props.put(USERNAME.key(), username);
			props.put(PASSWORD.key(), pwd);

			return new CatalogTableImpl(
				tableSchema,
				props,
				""
			);
		} catch (Exception e) {
			throw new CatalogException(
				String.format("Failed getting table %s", tablePath.getFullName()), e);
		}
	}

	//TODO: 补全MySQL的类型
	public static final String MYSQL_VARCHAR = "varchar";
	public static final String MYSQL_BIGINT = "bigint";
	public static final String MYSQL_BIGINT_UNSIGNED = "bigint unsigned";
	public static final String MYSQL_LONGTEXT = "longtext";
	public static final String MYSQL_DATETIME = "datetime";
	public static final String MYSQL_INT = "int";
	public static final String MYSQL_INTEGER = "integer";
	public static final String MYSQL_INT_UNSIGNED = "int unsigned";
	public static final String MYSQL_TINYINT = "tinyint";
	public static final String MYSQL_TINYINT_UNSIGHED = "tinyint unsigned";
	public static final String MYSQL_DECIMAL = "decimal";
	public static final String MYSQL_NUMERIC = "numeric";
	public static final String MYSQL_DOUBLE = "double";
	public static final String MYSQL_BLOB = "blob";
	public static final String MYSQL_SMALLINT = "smallint";
	public static final String MYSQL_SMALLINT_UNSIGHED = "smallint unsigned";
	public static final String MYSQL_TEXT = "text";
	public static final String MYSQL_MEDIUMTEXT = "mediumtext";
	public static final String MYSQL_LONG_BLOB = "longblob";
	public static final String MYSQL_TIMESTAMP = "timestamp";
	public static final String MYSQL_CHAR = "char";
	public static final String MYSQL_BINARY = "binary";
	public static final String MYSQL_BIT = "bit";
	public static final String MYSQL_MEDIUMBLOB = "mediumblob";
	public static final String MYSQL_VARBINARY = "varbinary";
	public static final String MYSQL_DATE = "date";
	public static final String MYSQL_SET = "set";
	public static final String MYSQL_ENUM = "enum";
	public static final String MYSQL_FLOW = "float";
	public static final String MYSQL_TIME = "time";

	private DataType fromJDBCType(String dataType, Long charLength, Long numPrecision, Long numScale, Long datetimePrecision, boolean isPrimaryKey) throws SQLException {
		DataType rst = null;
		switch(dataType) {
			case MYSQL_INT:
			case MYSQL_INTEGER:
				rst = DataTypes.INT();
				break;
			case MYSQL_TINYINT:
			case MYSQL_TINYINT_UNSIGHED:
				rst =  DataTypes.TINYINT();
				break;
			case MYSQL_SMALLINT:
			case MYSQL_SMALLINT_UNSIGHED:
				rst =  DataTypes.SMALLINT();
				break;
			case MYSQL_BIGINT:
			case MYSQL_BIGINT_UNSIGNED:
			case MYSQL_INT_UNSIGNED:
				rst =  DataTypes.BIGINT();
				break;
			case MYSQL_VARCHAR:
				rst =  DataTypes.VARCHAR(charLength.intValue());
				break;
			case MYSQL_LONGTEXT:
			case MYSQL_TEXT:
			case MYSQL_MEDIUMTEXT:
				rst =  DataTypes.STRING();
				break;
			case MYSQL_DATETIME:
				rst =  DataTypes.TIMESTAMP(datetimePrecision.intValue());
				break;
			case MYSQL_DECIMAL:
			case MYSQL_NUMERIC:
				if (numPrecision <= 0) {
					numPrecision = 1L;
				}
				rst =  DataTypes.DECIMAL(numPrecision.intValue(), numScale.intValue());
				break;
			case MYSQL_DOUBLE:
				rst =  DataTypes.DOUBLE();
				break;
			default:
				throw new UnsupportedOperationException(
					String.format("Doesn't support MySQL type '%s' yet", dataType));

		}
		if (isPrimaryKey) {
			rst = rst.notNull();
		}
		return rst;
	}

	@Override
	public boolean tableExists(ObjectPath tablePath) throws CatalogException {
		List<String> tables = null;
		try {
			tables = listTables(tablePath.getDatabaseName());
		} catch (DatabaseNotExistException e) {
			return false;
		}

		return tables.contains(tablePath.getObjectName());
	}
}
