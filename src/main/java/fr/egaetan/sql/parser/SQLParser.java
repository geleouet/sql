package fr.egaetan.sql.parser;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.Table.Values;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;
import fr.egaetan.sql.query.Query;
import fr.egaetan.sql.query.Query.QueryFrom;
import fr.egaetan.sql.result.Resultat;
import fr.egaetan.sql.result.Resultat.ResultatBuilder;
import fr.egaetan.sql.result.Resultat.ResultatColumn;
import fr.egaetan.sql.sample.PrenomListe;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.UnionOp;
import net.sf.jsqlparser.statement.update.Update;

public class SQLParser {

	private static final String SetOperationList = null;
	static Base base = new Base();
	static {
		createTableClient(base);
		createTableMembers(base);
	}

	private static String generateRandom(String chars, int len, Random random) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < len; i++) {
			int randomIndex = random.nextInt(chars.length());
			sb.append(chars.charAt(randomIndex));
		}

		return sb.toString();
	}
	
	private static String generateCode() {
		SecureRandom random = new SecureRandom();
		String from = generateRandom("0123456789", 10, random)
				 + generateRandom("abcdefghijklmnopqrstuvwxyz", 26, random);
		
		List<Integer> indexes = IntStream.range(0, from.length()).mapToObj(i -> i).collect(Collectors.toList());
		java.util.Collections.shuffle(indexes, random);
		
		String p = "";
		for (int i = 0; i < 8; i++) {
			p += from.charAt(indexes.get(i));
		}		
		return p;
	}

	
	public static Table createTableMembers(Base base) {
		Table tableClient = base
				.createTable("members")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("username", ColumnType.STRING)
				.addColumn("password", ColumnType.STRING)
				.build();
		List<String> prenoms = new PrenomListe().firstnames();
		for (int i = 0; i < prenoms.size(); i++) {
			String prenom = prenoms.get(i);
			tableClient.insert(
					tableClient.values()
					.set("id", i)
					.set("username", prenom)
					.set("password", generateCode())
					);
		}
		
		return tableClient;
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
		
		if (select.getSelectBody() instanceof PlainSelect) {
			return plainSelect(select.getSelectBody());
		}
		else if (select.getSelectBody() instanceof SetOperationList) {
			SetOperationList opList = (SetOperationList) select.getSelectBody();
			if (opList.getOperations().size() == 1 && opList.getOperations().get(0) instanceof UnionOp) {
				Resultat res = null;
				for (SelectBody s : opList.getSelects()) {
					Resultat r = plainSelect(s);
					if (res == null) {
						res = r;
					}
					else {
						res.values().addAll(r.values());
					}
				}
				return res;
			}
		}
		throw new UnsupportedOperationException("Unknown select " + select);		
	}

	private Resultat plainSelect(SelectBody selectBody) {
		PlainSelect plainSelect = (PlainSelect) selectBody;
		System.out.println(plainSelect.getFromItem());
		
		FromItem fromItem = plainSelect.getFromItem();
		net.sf.jsqlparser.schema.Table tableItem = ((net.sf.jsqlparser.schema.Table) fromItem);
		
		if (fromItem == null) {
			System.out.println();
			if (plainSelect.getSelectItems().size() == 1) {
				SelectItem selectItem = plainSelect.getSelectItems().get(0);
				String selectionString = ((SelectExpressionItem) selectItem).getExpression().toString();
				if ("version()".equals(selectionString)) {
					ResultatBuilder rb =  new ResultatBuilder(List.of(new ResultatColumn("version", "version", ColumnType.STRING)));
					rb.addRow(new Object[]{"LazySloDB 0.1"});
					return rb.build();
				}
			}
		}
		
		if (tableItem == null) {
			List<Object> res = new ArrayList<>();
			List<Column> columns = new ArrayList<>();
			for (SelectItem si : plainSelect.getSelectItems()) {
				if (si instanceof SelectExpressionItem) {
					SelectExpressionItem sei = (SelectExpressionItem) si;
					if (sei.getExpression() instanceof StringValue) {
						res.add(((StringValue) sei.getExpression()).getValue());
						columns.add(new ResultatColumn("?", "?", ColumnType.STRING));
					}
					else if (sei.getExpression() instanceof DoubleValue) {
						res.add(((DoubleValue) sei.getExpression()).getValue());
						columns.add(new ResultatColumn("?", "?", ColumnType.ENTIER));
					}
					else if (sei.getExpression() instanceof LongValue) {
						res.add(((LongValue) sei.getExpression()).getValue());
						columns.add(new ResultatColumn("?", "?", ColumnType.ENTIER));
					}
				}
				else {
					throw new UnsupportedOperationException("Cannot select " + si);
				}
			}
			ResultatBuilder resultatBuilder = new ResultatBuilder(columns);
			resultatBuilder.addRow(res.toArray());
			return resultatBuilder.build(); 
		}
		
		Table table = tableItem.getSchemaName() != null ? base.table(tableItem.getName(), tableItem.getSchemaName()) : base.table(tableItem.getName());
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
			else if (selectItem instanceof AllColumns) {
				cols.addAll(table.columns());
			}
		}

		QueryFrom query = new Query().select(cols.toArray(i -> new Column[i])).from(table);
		if (plainSelect.getWhere() != null) {
			System.out.println("Where condition: " + plainSelect.getWhere().toString());
			Expression where = plainSelect.getWhere();
			query = whereToken(table, query, where);
		}
		
		Resultat res = query.execute();
		res.aliases(colsAliases);
		return res;
	}

	private QueryFrom whereToken(Table table, QueryFrom query, Expression where) {
		if (where instanceof EqualsTo) {
			EqualsTo eq = (EqualsTo) where;
			if (eq.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
				net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) eq.getLeftExpression();
				Column column = table.column(c.getColumnName());
				if (eq.getRightExpression() instanceof LongValue) {
					query = query.where(column).isEqualTo(((LongValue) eq.getRightExpression()).getValue());
				}
				if (eq.getRightExpression() instanceof StringValue) {
					query = query.where(column).isEqualTo(((StringValue) eq.getRightExpression()).getValue());
				}
			}
			else if (eq.getLeftExpression() instanceof LongValue) {
				Column column = new Column() {

					@Override
					public boolean need(Column c) {
						return true;
					}
					@Override
					public String displayName() {
						return ""+((LongValue) eq.getLeftExpression()).getValue();
					}

					@Override
					public ColumnType type() {
						return ColumnType.ENTIER;
					}
					@Override
					public Object read(Object[] datas) {
						return ((LongValue) eq.getLeftExpression()).getValue();
					}	
				};
				if (eq.getRightExpression() instanceof LongValue) {
					query = query.where(column).isEqualTo(((LongValue) eq.getRightExpression()).getValue());
				}
				if (eq.getRightExpression() instanceof StringValue) {
					query = query.where(column).isEqualTo(((StringValue) eq.getRightExpression()).getValue());
				}
			}
			else if (eq.getLeftExpression() instanceof StringValue) {
				Column column = new Column() {
					@Override
					public boolean need(Column c) {
						return true;
					}
					@Override
					public String displayName() {
						return ""+((StringValue) eq.getLeftExpression()).getValue();
					}
					
					@Override
					public ColumnType type() {
						return ColumnType.STRING;
					}
					@Override
					public Object read(Object[] datas) {
						return ((StringValue) eq.getLeftExpression()).getValue();
					}	
				};
				if (eq.getRightExpression() instanceof LongValue) {
					query = query.where(column).isEqualTo(((LongValue) eq.getRightExpression()).getValue());
				}
				if (eq.getRightExpression() instanceof StringValue) {
					query = query.where(column).isEqualTo(((StringValue) eq.getRightExpression()).getValue());
				}
			}
		}
		else  if (where instanceof MinorThan) {
			MinorThan eq = (MinorThan) where;
			if (eq.getLeftExpression() instanceof net.sf.jsqlparser.schema.Column) {
				net.sf.jsqlparser.schema.Column c = (net.sf.jsqlparser.schema.Column) eq.getLeftExpression();
				Column column = table.column(c.getColumnName());
				if (eq.getRightExpression() instanceof LongValue) {
					query = query.where(column).isLessThan(((LongValue) eq.getRightExpression()).getValue());
				}
				if (eq.getRightExpression() instanceof StringValue) {
					query = query.where(column).isLessThan(((StringValue) eq.getRightExpression()).getValue());
				}
			}
		}
		else if (where instanceof AndExpression) {
			AndExpression ae = (AndExpression) where;
			query.and(null);
			QueryFrom left = whereToken(table, query, ae.getLeftExpression());
			QueryFrom right = whereToken(table, query, ae.getRightExpression());
			query = right;
		}
		else if (where instanceof OrExpression) {
			OrExpression ae = (OrExpression) where;
			query.or(null);
			QueryFrom left = whereToken(table, query, ae.getLeftExpression());
			QueryFrom right = whereToken(table, query, ae.getRightExpression());
			query = right;
		}
		else if (where instanceof Parenthesis) {
			Parenthesis ae = (Parenthesis) where;
			QueryFrom parenthesis = query.derivate();
			whereToken(table, parenthesis, ae.getExpression());
			query.addPredicate(parenthesis.predicate());
		}
		else {
			throw new RuntimeException("Unknown Where " +where);
		}
		return query;
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
