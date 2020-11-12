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
			DatabaseMetaData metaData = conn.getMetaData();
			Optional<UniqueConstraint> primaryKey = getPrimaryKey(
				metaData,
				tablePath.getDatabaseName(),
				tablePath.getObjectName());

			PreparedStatement ps = conn.prepareStatement(
				String.format("SELECT * FROM %s;", tablePath.getFullName()));

			ResultSetMetaData rsmd = ps.getMetaData();

			String[] names = new String[rsmd.getColumnCount()];
			DataType[] types = new DataType[rsmd.getColumnCount()];

			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				names[i - 1] = rsmd.getColumnName(i);
				types[i - 1] = fromJDBCType(rsmd, i);
				if (rsmd.isNullable(i) == ResultSetMetaData.columnNoNulls) {
					types[i - 1] = types[i - 1].notNull();
				}
			}

			TableSchema.Builder tableBuilder = new TableSchema.Builder()
				.fields(names, types);
			primaryKey.ifPresent(pk ->
				tableBuilder.primaryKey(pk.getName(), pk.getColumns().toArray(new String[0]))
			);
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

	private DataType fromJDBCType(ResultSetMetaData metadata, int colIndex) throws SQLException {
		String dataType = metadata.getColumnTypeName(colIndex).toLowerCase();
		int precision = metadata.getPrecision(colIndex);
		int scale = metadata.getScale(colIndex);
		switch(dataType) {
			case MYSQL_INT:
			case MYSQL_INTEGER:
				return DataTypes.INT();
			case MYSQL_TINYINT:
			case MYSQL_TINYINT_UNSIGHED:
				return DataTypes.TINYINT();
			case MYSQL_SMALLINT:
			case MYSQL_SMALLINT_UNSIGHED:
				return DataTypes.SMALLINT();
			case MYSQL_BIGINT:
			case MYSQL_BIGINT_UNSIGNED:
			case MYSQL_INT_UNSIGNED:
				return DataTypes.BIGINT();
			case MYSQL_VARCHAR:
				return DataTypes.VARCHAR(precision);
			case MYSQL_LONGTEXT:
			case MYSQL_TEXT:
			case MYSQL_MEDIUMTEXT:
				return DataTypes.STRING();
			case MYSQL_DATETIME:
				return DataTypes.TIMESTAMP(3);
			case MYSQL_DECIMAL:
			case MYSQL_NUMERIC:
				return DataTypes.DECIMAL(precision, scale);
			default:
				throw new UnsupportedOperationException(
					String.format("Doesn't support MySQL type '%s' yet", dataType));

		}
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
