package fr.egaetan.sql.base;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.common.Column;

public class TableColumn implements Column {
	
	String name;
	ColumnType type;
	String qualifiedName;

	public TableColumn(String name, String qualifiedName, ColumnType type) {
		this.name = name;
		this.qualifiedName = qualifiedName;
		this.type = type;
	}

	@Override
	public String displayName() {
		return name;
	}

	@Override
	public String qualifiedName() {
		return qualifiedName;
	}
	
	@Override
	public  ColumnType type() {
		return type;
	}
}