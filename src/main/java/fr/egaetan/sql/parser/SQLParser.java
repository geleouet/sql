package fr.egaetan.sql.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.Table.Values;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;
import fr.egaetan.sql.query.Query;
import fr.egaetan.sql.query.Query.QueryFrom;
import fr.egaetan.sql.result.Resultat;
import fr.egaetan.sql.sample.PrenomListe;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.Update;

public class SQLParser {

	static Base base = new Base();
	static {
		createTableClient(base);
	}

	private static Table createTableClient(Base base) {
		Table tableClient = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("prenom", ColumnType.STRING)
				.addColumn("color", ColumnType.ENTIER)
				.addColumn("city", ColumnType.ENTIER)
				.build();
		List<String> prenoms = new PrenomListe().firstnames();
		for (int i = 0; i < prenoms.size(); i++) {
			String prenom = prenoms.get(i);
			tableClient.insert(
					tableClient.values()
						.set("id", i)
						.set("prenom", prenom)
						.set("color", i % 9)
						.set("city", (i*3 + 7) % 10)
					);
		}
		
		return tableClient;
	}
	
	public Resultat parse(String sql) {
		System.out.println(sql);
		try {
			Statement statement = CCJSqlParserUtil.parse(sql);
			if (statement instanceof Select) {
				return parseSelect((Select) statement);
			}
		} catch (JSQLParserException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	private Resultat parseSelect(Select select) {
		PlainSelect plainSelect = (PlainSelect) ((Select) select).getSelectBody();
		System.out.println(plainSelect.getFromItem());
		
		FromItem fromItem = plainSelect.getFromItem();
		net.sf.jsqlparser.schema.Table tableItem = ((net.sf.jsqlparser.schema.Table) fromItem);
		
		Table table = base.table(tableItem.getName());
		Map<String, Table> aliases = new HashMap<>();
		if (tableItem.getAlias() != null) {
			String name = tableItem.getAlias().getName();
			aliases.put(name, table);
		}
		
		
		List<SelectItem> selectCols = plainSelect.getSelectItems();
		List<Column> cols = new ArrayList<>();
		List<String> colsAliases = new ArrayList<>();
		for (SelectItem selectItem : selectCols) {
			System.out.println(selectItem.toString());
			if (selectItem instanceof SelectExpressionItem) {
				SelectExpressionItem selectExpItem = (SelectExpressionItem) selectItem;
				Expression expression = selectExpItem.getExpression();
				String name = "";
				if (expression instanceof net.sf.jsqlparser.schema.Column) {
					net.sf.jsqlparser.schema.Column col = (net.sf.jsqlparser.schema.Column) expression;
					String columnName = col.getColumnName();
					name = columnName;
					cols.add(table.column(columnName));
				}
				if (selectExpItem.getAlias() != null) {
					name = selectExpItem.getAlias().getName();
				}
				colsAliases.add(name);
			}
		}

		QueryFrom query = new Query().select(cols.toArray(i -> new Column[i])).from(table);
		if (plainSelect.getWhere() != null) {
			System.out.println("Where condition: " + plainSelect.getWhere().toString());
			Expression where = plainSelect.getWhere();
			
			
			if (where instanceof EqualsTo) {
				EqualsTo eq = (EqualsTo) where;
				if (eq.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
					net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) eq.getLeftExpression();
					Column column = table.column(c.getColumnName());
					if (eq.getRightExpression() instanceof LongValue) {
						query = query.where(column).isEqualTo(((LongValue) eq.getRightExpression()).getValue());
					}
				}
				System.out.println();
			}
		}
		
		Resultat res = query.execute();
		res.aliases(colsAliases);
		return res;
	}

	public int parseUpdate(String sql) {
		try {
			Statement statement = CCJSqlParserUtil.parse(sql);
			if (statement instanceof Update) {
				return parseUpdate((Update) statement);
			}
		} catch (JSQLParserException e) {
			throw new RuntimeException(e);
		}
		// TODO Auto-generated method stub
		return 0;
	}

	private int parseUpdate(Update update) {
		net.sf.jsqlparser.schema.Table tableUpdate = update.getTable();
		Table table = base.table(tableUpdate.getName());
		
		List<Predicate<DataRow>> predicate = new ArrayList<>();
		if (update.getWhere() != null) {
			Expression where = update.getWhere();
			if (where instanceof EqualsTo) {
				EqualsTo eq = (EqualsTo) where;
				if (eq.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
					net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) eq.getLeftExpression();
					Column column = table.column(c.getColumnName());
					if (eq.getRightExpression() instanceof LongValue) {
						predicate.add(r -> equal(column.read(r.data()), ((LongValue) eq.getRightExpression()).getValue()));
					}
					else if (eq.getRightExpression() instanceof StringValue) {
						predicate.add(r -> equal(column.read(r.data()), ((StringValue) eq.getRightExpression()).getValue()));
					}
				}
			}
		}
		
		Values values = new Values(table);
		for (var u:update.getUpdateSets()) {
			System.out.println(u);
			ArrayList<net.sf.jsqlparser.schema.Column> columns = u.getColumns();
			ArrayList<Expression> expressions = u.getExpressions();
			Expression expression = expressions.get(0);
			if (expression instanceof LongValue) {
				String colName = columns.get(0).getColumnName();
				values.set(colName, ((LongValue) expression).getValue());
			}
			else if (expression instanceof StringValue) {
				String colName = columns.get(0).getColumnName();
				values.set(colName, ((StringValue) expression).getValue());
			}
		}
		
		AtomicInteger a = new AtomicInteger();
		table.datas().forEach(r -> {
			if (predicate.stream().allMatch(p -> p.test(r))) {
				a.incrementAndGet();
				r.update(values);
			}
		});
		return a.get();
	}

	private boolean equal(Object read, String value) {
		if (read instanceof String) {
			String s = (String) read;
			return s.equals(value);
			
		}
		return false;
	}

	private boolean equal(Object read, long l) {
		if (read instanceof Number) {
			long r =   ((Number) read) .longValue();
			return l == r;
		}
		return false;
	}
}
