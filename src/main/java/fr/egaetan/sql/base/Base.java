package fr.egaetan.sql.base;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.Table.TableBuilder;

public class Base {
	
	static final String CATALOG = "LazySlow_"+new SimpleDateFormat("ddMMyyyy").format(new Date());
	ConcurrentMap<String, Table> tables = new ConcurrentHashMap<>();

	Table catalog;
	{
		catalog = createTable("information_schema.tables")
		.addColumn("table_catalog", ColumnType.STRING)
		.addColumn("table_schema", ColumnType.STRING)
		.addColumn("table_name", ColumnType.STRING)
		.addColumn("table_type", ColumnType.STRING)
		.addColumn("self_referencing_column_name", ColumnType.STRING)
		.addColumn("reference_generation", ColumnType.STRING)
		.addColumn("user_defined_type_catalog", ColumnType.STRING)
		.addColumn("user_defined_type_schema", ColumnType.STRING)
		.addColumn("user_defined_type_name", ColumnType.STRING)
		.addColumn("is_insertable_into", ColumnType.STRING)
		.addColumn("is_typed", ColumnType.STRING)
		.addColumn("commit_action", ColumnType.STRING)
		.build();
		catalog(catalog);
	}
	
	
	
	public static Base create() {
		return new Base();
	}

	public TableBuilder createTable(String name) {
		return new TableBuilder(name, this);
	}

	public Table register(String name, Table table) {
		tables.put(name.toUpperCase(), table);
		catalog(table);
		return table;
	}

	private void catalog(Table table) {
		if (catalog != null) {
			Table.Values values = new Table.Values(catalog);
			values.set("table_catalog", CATALOG);
			values.set("table_schema", "public");
			values.set("table_name", table.name());
			values.set("table_type", "BASE_TABLE");
			values.set("is_insertable_into", "yes");
			values.set("is_typed", "no");
			catalog.insert(values);
		}
	}

	public Table table(String tableName) {
		String key = tableName.toUpperCase();
		System.out.println(key);
		System.out.println(tables);
		return tables.get(key);
	}
	public Table table(String tableName, String schema) {
		return table(schema+"."+tableName);
	}

}
