package fr.egaetan.sql.base;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import fr.egaetan.sql.base.Table.TableBuilder;
import net.sf.jsqlparser.statement.select.FromItem;

public class Base {
	
	ConcurrentMap<String, Table> tables = new ConcurrentHashMap<>();

	public static Base create() {
		return new Base();
	}

	public TableBuilder createTable(String name) {
		return new TableBuilder(name, this);
	}

	public Table register(String name, Table table) {
		tables.put(name.toUpperCase(), table);
		return table;
	}

	public Table table(String tableName) {
		return tables.get(tableName.toUpperCase());
	}

}
