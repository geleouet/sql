package fr.egaetan.sql.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.TableSelect;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.exception.TableNameSpecifiedMoreThanOnce;
import fr.egaetan.sql.executor.QueryExecutor;
import fr.egaetan.sql.executor.QueryExecutor.Explain;
import fr.egaetan.sql.result.Resultat;

public class Query {

	public static interface RowPredicate {
		
		public Column reference();

		public boolean valid(Object object);
		
		
	}
	public static class EqualsPredicate implements RowPredicate {

		private Column column;
		private Object value;

		public EqualsPredicate(Column column, Object value) {
			this.column = column;
			this.value = value;
		}

		@Override
		public boolean valid(Object data) {
			if (value instanceof Long) {
				if (value instanceof Number) {
					return ((Long) value).longValue() == ((Number) data).longValue();
				}
			}
			return value.equals(data);
		}

		
		@Override
		public String toString() {
			return "Filter: ("+column.qualifiedName() + " = " + value.toString()+")";
		}

		@Override
		public Column reference() {
			return column;
		}
		
	}

	public static class QueryPredicate {

		private Object value;
		private Column column;

		public QueryPredicate(Column column, Object value) {
			this.column = column;
			this.value = value;
		}

		public List<RowPredicate> predicates(TableSelect table) {
			if (table.has(column)) {
				return List.of(new EqualsPredicate(column, value));
			}
			return Collections.emptyList();
		}

	}

	public static class QueryWhere {

		private Column column;
		private QueryFrom queryFrom;

		public QueryWhere(QueryFrom queryFrom, Column column) {
			this.queryFrom = queryFrom;
			this.column = column;
		}

		public QueryFrom isEqualTo(Object o) {
			queryFrom.addPredicate(new QueryPredicate(column, o));
			return queryFrom;
		}

	}
	

	public static class QueryFrom {
		
		private List<TableSelect> tables;
		private QuerySelect querySelect;
		private List<QueryPredicate> queryPredicates;
		private List<QueryPredicateJoin> queryJoinPredicates;

		public QueryFrom(QuerySelect querySelect, TableSelect ... tables) {
			this.tables = new ArrayList<>(Arrays.asList(tables));
			this.querySelect = querySelect;
			this.queryPredicates = new ArrayList<>();
			this.queryJoinPredicates = new ArrayList<>();
		}

		public void addPredicate(QueryPredicate queryPredicate) {
			queryPredicates.add(queryPredicate);
		}

		public QueryWhere where(Column column) {
			return new QueryWhere(this, column);
		}


		public QueryWhere and(Column column) {
			return where(column);
		}

		public QueryJoin innerJoin(TableSelect tableSelect) {
			if (tables.stream().anyMatch(t -> t.name().equalsIgnoreCase(tableSelect.name()))) {
				throw new TableNameSpecifiedMoreThanOnce(tableSelect.name());
			}
			
			return new QueryJoin(this, tableSelect);
		}

		public Resultat execute() {
			QueryExecutor queryExecutor = new QueryExecutor(tables, querySelect, queryPredicates, queryJoinPredicates);
			return queryExecutor.execute();
		}

		public QueryExplain explain() {
			return new QueryExplain(new QueryExecutor(tables, querySelect, queryPredicates, queryJoinPredicates));
		}
		
	}
	
	public static class QueryExplain {
		private QueryExecutor queryExecutor;

		public QueryExplain(QueryExecutor queryExecutor) {
			this.queryExecutor = queryExecutor;
		}

		public Explain execute() {
			return queryExecutor.explain();
		}

		public QueryExplainAnalyse analyse() {
			return new QueryExplainAnalyse(queryExecutor);
		}
	}
	public static class QueryExplainAnalyse {
		private QueryExecutor queryExecutor;
		
		public QueryExplainAnalyse(QueryExecutor queryExecutor) {
			this.queryExecutor = queryExecutor;
		}
		
		public Explain execute() {
			
			return queryExecutor.explain();
		}
		
	}
	
	
	public static class QueryPredicateJoin {
		private final Column a;
		private final Column b;
		
		public QueryPredicateJoin(Column a, Column b, PredicateJoin predicate) {
			super();
			this.a = a;
			this.b = b;
		}
		
		public Column getA() {
			return a;
		}

		public Column getB() {
			return b;
		}
		
	}
	
	public static class QueryJoinOn {

		private QueryFrom queryFrom;
		private TableSelect table;
		private Column column;

		public QueryJoinOn(QueryFrom queryFrom, TableSelect table, Column column) {
			this.queryFrom = queryFrom;
			this.table = table;
			this.column = column;
		}

		public QueryFrom isEqualTo(Column column) {
			queryFrom.queryJoinPredicates.add(new QueryPredicateJoin(this.column, column, (a,b) -> a.equals(b)));
			queryFrom.tables.add(table);
			return queryFrom;
		}
		
	}
	public static class QueryJoin {

		private QueryFrom queryFrom;
		private TableSelect table;

		public QueryJoin(QueryFrom queryFrom, TableSelect table) {
			this.queryFrom = queryFrom;
			this.table = table;
			
		}

		public  QueryJoinOn on(Column column) {
			return new QueryJoinOn(queryFrom, table, column);
		}
		
	}
	
	public static class QuerySelect {

		private Column[] columns;

		public QuerySelect(Column[] columns) {
			this.columns = columns;
		}
		
		public List<Column> columns(TableSelect table) {
			return Arrays.stream(columns).filter(c -> table.has(c)).collect(Collectors.toList());
		}

		public QueryFrom from(TableSelect ... tables) {
			return new QueryFrom(this, tables);
		}

		public List<Column> columns() {
			return Arrays.asList(this.columns);
		}

	}

	public QuerySelect select(Column ...columns) {
		return new QuerySelect(columns);
	}
	
	public static interface EntierFunction {
		Integer apply(Integer i);
	}
	public static interface StringFunction {
		String apply(String i);
	}

	public static Column compound(Column column, String name, EntierFunction object) {
		return new Column() {
			
			@Override
			public boolean need(Column c) {
				return column.need(c);
			}
			
			@Override
			public ColumnType type() {
				return ColumnType.ENTIER;
			}
			
			@Override
			public String displayName() {
				return name;
			}
			
			@Override
			public Column[] references() {
				return new Column[] {column};
			}
			
			@Override
			public Object read(Object[] datas) {
				return object.apply((Integer) datas[0]);
			}
		};
	}
	public static Column compound(Column column, String name, StringFunction object) {
		return new Column() {
			
			@Override
			public boolean need(Column c) {
				return column.need(c);
			}
			
			@Override
			public ColumnType type() {
				return ColumnType.STRING;
			}
			
			@Override
			public String displayName() {
				return name;
			}
			
			@Override
			public Column[] references() {
				return new Column[] {column};
			}
			
			@Override
			public Object read(Object[] datas) {
				return object.apply((String) datas[0]);
			}
		};
	}

}
