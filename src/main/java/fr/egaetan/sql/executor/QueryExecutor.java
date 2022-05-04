package fr.egaetan.sql.executor;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import fr.egaetan.sql.base.Table.Values;
import fr.egaetan.sql.base.TableSelect;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;
import fr.egaetan.sql.exception.ColumnDoesntExist;
import fr.egaetan.sql.exception.UnsupportedOperation;
import fr.egaetan.sql.query.Query.QueryPredicate;
import fr.egaetan.sql.query.Query.QueryPredicateJoin;
import fr.egaetan.sql.query.Query.QuerySelect;
import fr.egaetan.sql.query.Query.RowPredicate;
import fr.egaetan.sql.result.Resultat;
import fr.egaetan.sql.result.Resultat.ResultatBuilder;
import fr.egaetan.sql.result.Resultat.ResultatRow;

public class QueryExecutor {

	
	private List<TableSelect> tables;
	private QuerySelect querySelect;
	private List<QueryPredicate> queryPredicates;
	private List<QueryPredicateJoin> queryJoinPredicates;


	public QueryExecutor(List<TableSelect> tables, QuerySelect querySelect, List<QueryPredicate> queryPredicates, List<QueryPredicateJoin> queryJoinPredicates) {
		super();
		this.tables = tables;
		this.querySelect = querySelect;
		this.queryPredicates = queryPredicates;
		this.queryJoinPredicates = queryJoinPredicates;
	}

	
	public interface Children {
		
		Stream<? extends DataRow> execute();
		List<? extends Column> columns();
		String explain(String indent);
		
	}

	public static class SeqScan implements Children {
		TableSelect table;
		private List<RowPredicate> predicates;
		private List<NestedLoopFilter> filters;
		private ExecutionReport report = new ExecutionReport();
		
		public SeqScan(TableSelect table, List<RowPredicate> predicates) {
			super();
			this.table = table;
			this.predicates = predicates;
			filters = predicates.stream().map(this::buildFilter).collect(Collectors.toList());
		}

		public NestedLoopFilter buildFilter(RowPredicate filter) {
			for (int j = 0; j < columns().size(); j++) {
				if (columns().get(j).qualified().identify(filter.reference().qualified())) {
					int j$  =j;
					return row -> filter.valid(row.data()[j$]);
				}
			}
			throw new ColumnDoesntExist(filter.reference().displayName());
		}
		
		@Override
		public Stream<? extends DataRow> execute() {
			report.execute();
			Stream<? extends DataRow> resultat = table.datas().filter(this::filter).peek(__ -> report.count());
			report.end();
			return resultat;
		}

		private boolean filter(DataRow row) {
			boolean allMatch = filters.stream().allMatch(p -> p.accept(row));
			if (!allMatch) {
				report.removed();
			}
			return allMatch;
		}
		
		@Override
		public List<? extends Column> columns() {
			return table.columns();
		}
		
		@Override
		public String explain(String indent) {
			String executionReport = report.explain(indent+ "    ");
			return indent + "-> Seq Scan on " + table.name() + report.executionStatistics()
			+ (predicates.size() > 0 ? "\n" + predicates.stream().map(p -> indent +"    "+ p.toString()).collect(Collectors.joining("\n")) : "")
			+ (predicates.size() > 0 ? (executionReport.isEmpty() ? "" : ("\n" + executionReport)) : "");
		}
	}
	
	public static class BiDataRow implements DataRow {
		DataRow first;
		DataRow snd;
		
		public BiDataRow(DataRow first, DataRow snd) {
			super();
			this.first = first;
			this.snd = snd;
		}

		@Override
		public Object[] data() {
			Object[] res = new Object[first.data().length + snd.data().length];
			System.arraycopy(first.data(), 0, res, 0, first.data().length);
			System.arraycopy(snd.data(), 0, res, first.data().length, snd.data().length);
			return res;
		}

		@Override
		public void update(Values values) {
			throw new UnsupportedOperation();
		}
	}
	
	public static interface NestedLoopFilter {
		boolean accept(DataRow data);
	}
	
	
	public static class ExecutionReport {
		private int rowRemovedByFilter = 0;
		private int loop = 0;
		private int rows = 0;
		private long time = 0;
		private long start;
		
		public void count() {
			rows++;
		}
		
		public void removed() {
			rowRemovedByFilter++;
		}
		
		public void execute() {
			loop++;
			start = System.nanoTime();
		}

		public void end() {
			long elapsed = System.nanoTime() - start;
			time += elapsed;
		}
		
		public String explain(String indent) {
			if (loop == 0) {
				return ""; 
			}
			return indent  + "Rows Removed by Filter: " + rowRemovedByFilter;
		}

		public String executionStatistics() {
			NumberFormat f = new DecimalFormat("###.##");
			if (loop == 0) {
				return "";
			}
			return "  (actual time=" + f.format(time / 1_000_000.) + " rows=" + rows/loop +" loops="+loop + ")";
		}

		
	}
	
	
	public static class NestedLoop implements Children {

		Children first;
		Children snd;
		private List<? extends Column> columns;
		private List<JoinFilter> filters;
		private List<NestedLoopFilter> loopFilters;
		private ExecutionReport report = new ExecutionReport();
		
		public NestedLoop(Children first, Children snd, List<JoinFilter> filters) {
			super();
			this.first = first;
			this.snd = snd;
			this.filters = filters;
			this.columns = List.of(first.columns(), snd.columns()).stream().flatMap(List::stream).collect(Collectors.toList());
			this.loopFilters = filters.stream().map(this::buildFilter).collect(Collectors.toList());
		}
		
		public NestedLoopFilter buildFilter(JoinFilter filter) {
			var q = new Object() {
				int a;
				int b;
			};
			
			for (int j = 0; j < columns().size(); j++) {
				if (columns().get(j).qualified().identify(filter.first.qualified())) {
					q.a = j;
				}
				if (columns().get(j).qualified().identify(filter.snd.qualified())) {
					q.b = j;
				}
			}
			return row -> filter.accept(row.data()[q.a], row.data()[q.b]);
		}

		@Override
		public Stream<? extends DataRow> execute() {
			report.execute();
			Stream<BiDataRow> resultat = first.execute().flatMap(f -> 
					snd.execute()
						.map(s -> new BiDataRow(f, s))
						.filter(this::filter)
						)
					.peek(__ -> report.count());
			report.end();
			return resultat;
		}

		private boolean filter(BiDataRow d) {
			boolean allMatch = loopFilters.stream().allMatch(filter -> filter.accept(d));
			if (!allMatch) {
				report.removed();
			}
			return allMatch;
		}

		@Override
		public List<? extends Column> columns() {
			return columns;
		}

		@Override
		public String explain(String indent) {
			String executionReport = report.explain(indent + "    ");
			return indent + "-> Nested Loop" + report.executionStatistics() + "\n"
						+ filters.stream().map(j -> j.explain(indent + "    ")).collect(Collectors.joining("\n")) + (filters.size() > 0 ? "\n" : "")
						+ (executionReport.isEmpty() ? "" : (executionReport + "\n"))
						+ first.explain(indent + "    ") + "\n"
						+ snd.explain(indent + "    ")
						;
		}
		
	}
	
	
	public static class RowBuilder {
		Function<DataRow, DataRow> maper;

		public RowBuilder(Function<DataRow, DataRow> maper) {
			super();
			this.maper = maper;
		}

		public static RowBuilder build(Children scan, List<Column> columns) {
			int[][] mappers = new int[columns.size()][];
			
			for (int i = 0; i < columns.size(); i++) {
				Column columnResultat = columns.get(i);
				Column[] references = columnResultat.references();
				
				mappers[i] = new int[references.length];
				
				for (int k = 0; k < references.length; k++) {
					Column c = references[k];
					
					for (int j = 0; j < scan.columns().size(); j++) {
						if (scan.columns().get(j).qualified().identify(c.qualified())) {
							mappers[i][k] = j;
							break;
						}
					}
				}
			}
			
			RowBuilder rowBuilder = new RowBuilder(d ->  {
				Object[] source = d.data();
				Object[] data = new Object[columns.size()];
				for (int i = 0; i < columns.size(); i++) {
					Column column = columns.get(i);
					Object[] tmp = new Object[column.references().length];
					for (int j = 0; j < tmp.length; j++) {
						tmp[j] = source[mappers[i][j]];
					}
					data[i] = column.read(tmp);
				}
				return new ResultatRow(data);
			});
			return rowBuilder;
		}
	}
	
	public static class RowResultBuilder {
		Children from;
		RowBuilder builder;
		
		public RowResultBuilder(Children from, RowBuilder builder) {
			super();
			this.from = from;
			this.builder = builder;
		}

		public Stream<DataRow> execute() {
			return from.execute().map(builder.maper);
			
		}
	}
	
	public static class JoinFilter {
		Column first;
		Column snd;
		
		public JoinFilter(Column first, Column snd) {
			super();
			this.first = first;
			this.snd = snd;
		}

		public boolean accept(Object a, Object b) {
			return a.equals(b);
		}

		public String explain(String indent) {
			return indent + "Join Filter: (" + first.qualifiedName() + " = " + snd.qualifiedName() +")";
		}
	}
	
	public static class Explain implements Children {

		private final Children root;
		private long planningTime;
		private long executionTime = 0;
		
		
		public Explain(Children root) {
			super();
			this.root = root;
		}
		
		@Override
		public Stream<? extends DataRow> execute() {
			return root.execute();
		}

		@Override
		public List<? extends Column> columns() {
			return root.columns();
		}

		@Override
		public String explain(String indent) {
			NumberFormat f = new DecimalFormat("###.##");
			return root.explain(indent) 
					+ "\nPlanning Time: " + f.format(planningTime / 1_000_000.) + " ms"
					+ (executionTime !=0 ? "\nExecution Time: " + f.format(executionTime / 1_000_000.) + " ms" : "");
		}

		@Override
		public String toString() {
			return explain("");
		}


		public void setPlanningTime(long planningTime) {
			this.planningTime = planningTime;
		}

		public void setExecutionTime(long executionTime) {
			this.executionTime = executionTime;
		}
		
	}
	
	
	public Resultat execute() {
		long start = System.nanoTime();
		Explain current = explain();
		long stopExplain = System.nanoTime();
		current.setPlanningTime(stopExplain - start);
		
		RowResultBuilder resultBuilder = new RowResultBuilder(current, RowBuilder.build(current, querySelect.columns()));

		ResultatBuilder builder = new ResultatBuilder(querySelect.columns());
		resultBuilder.execute().forEach(d -> builder.addRow(d.data()));
		long stop = System.nanoTime();
		current.setExecutionTime(stop - stopExplain);
		
		Resultat build = builder.build();
		System.out.println(current);
		System.out.println(build);
		return build;
	}

	
	public static interface AnalyseResult {
		Explain explaination();
		Resultat resultat();
	}
	
	public Explain analyse() {
		Explain current = explain();
		RowResultBuilder resultBuilder = new RowResultBuilder(current, RowBuilder.build(current, querySelect.columns()));
		resultBuilder.execute().forEach(this::ignore);
		return current;
	}
	
	private void ignore(DataRow __) {
	}

	public Explain explain() {
		List<SeqScan> scans = new ArrayList<>();
		for (int i = 0; i < tables.size(); i++) {
			TableSelect table = tables.get(i);
			List<RowPredicate> predicates = new ArrayList<>();

			for (QueryPredicate queryPredicate : queryPredicates) {
				predicates.addAll(queryPredicate.predicates(table));
			}
			SeqScan seqScan = new SeqScan(table, predicates);
			scans.add(seqScan);
		}


		Children current = scans.get(0);
		for (int i = 1; i < tables.size(); i++) {
			List<JoinFilter> filters = new ArrayList<>();
			for (QueryPredicateJoin q : queryJoinPredicates) {
				// some algebra would help here
				for (int j = 0; j < i; j++) {
					if (tables.get(j).has(q.getA()) && tables.get(i).has(q.getB()) 
							|| tables.get(j).has(q.getB()) && tables.get(i).has(q.getA())) {
						filters.add(new JoinFilter(q.getA(), q.getB()));
					}
				}

			}
			current = new NestedLoop(current, scans.get(i), filters);
		}
		return new Explain(current);
	}

	
}
