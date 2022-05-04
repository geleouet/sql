package fr.egaetan.sql;

import java.util.List;
import java.util.stream.Collectors;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.SelectUtils;

public class DBParser {

	
	public static void main(String[] args) throws JSQLParserException {
		
		createTable();
		
		
		String s = "SELECT client.value FROM client INNER JOIN commande ON client.id = commande.id WHERE commande.id = 1";
		//String s = "SELECT value FROM client WHERE id = 1";
		Statement select = CCJSqlParserUtil.parse(s);
		System.out.println(select);
		System.out.println(select.getClass());

		
		System.out.println("List of  columns in select query");
        System.out.println("--------------------------------");
        List<SelectItem> selectCols = ((PlainSelect) ((Select) select).getSelectBody()).getSelectItems();
        for (SelectItem selectItem : selectCols)
            System.out.println(selectItem.toString());
        
        System.out.println(((Select) select).getSelectBody().getClass());
        List<Join> joins = ((PlainSelect) ((Select) select).getSelectBody()).getJoins();
        for (Join join : joins) {
        	System.out.println(join +" = " + join.getClass());
        	System.out.println(join.getRightItem());
        }
        
        
        System.out.println("Where condition: " + ((PlainSelect) ((Select) select).getSelectBody()).getWhere().toString());

	}

	private static void createTable() throws JSQLParserException {
		Base base = Base.create();
		Table tableClient = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		
		String s = "CREATE TABLE client (\n"
				+ " id INTEGER NOT NULL,\n"
				+ " value VARCHAR\n"
				+ ")";
		
		Statement parse = CCJSqlParserUtil.parse(s);
		System.out.println(parse.getClass());
		CreateTable create = (CreateTable) parse;
		System.out.println("Table name: " + create.getTable().getFullyQualifiedName());
		System.out.println("List of  columns in create query");
        System.out.println("--------------------------------");
        for (ColumnDefinition column  : create.getColumnDefinitions()) {
        	System.out.println(column.getColumnName());
        	System.out.println(column.getColDataType().getDataType());
        	if (column.getColumnSpecs() != null) {
        		System.out.println(column.getColumnSpecs().stream().collect(Collectors.joining(",")));
        	}
        }
        
	}
}
