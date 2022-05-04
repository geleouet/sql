package fr.egaetan.sql.result;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.Table.Values;
import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;
import fr.egaetan.sql.exception.ColumnDoesntExist;
import fr.egaetan.sql.exception.UnsupportedOperation;

public class Resultat {

	
	private long timeSpent;

	public static class ResultatColumn implements Column {

		private String name;
		private ColumnType type;
		private String qualifiedName;
		private String alias;

		public ResultatColumn(String name, String qualifiedName, ColumnType type) {
			this.name = name;
			this.qualifiedName = qualifiedName;
			this.type = type;
		}

		@Override
		public String displayName() {
			return alias == null ? name : alias;
		}

		public String qualifiedName() {
			return qualifiedName;
		}

		public String alias() {
			return alias;
		}

		public ColumnType type() {
			return type;
		}

		public void setAlias(String alias) {
			this.alias = alias;
		}

	}
	
	public static class ResultatRow implements DataRow {
		Object[] values;

		public ResultatRow(Object[] values) {
			super();
			this.values = values;
		}

		@Override
		public Object[] data() {
			return values;
		}

		@Override
		public void update(Values values) {
			throw new UnsupportedOperation();
		}

	}

	public static class ResultatBuilder {

		List<ResultatRow> rows = new ArrayList<>();
		List<ResultatColumn> columns = new ArrayList<>();
		private long startTime;

		public ResultatBuilder(List<? extends Column> columns) {
			this.startTime = System.nanoTime();
			this.columns = new ArrayList<>(columns.size());
			for (int i = 0; i < columns.size(); i++) {
				Column from = columns.get(i);
				this.columns.add(new ResultatColumn(from.displayName(), from.qualifiedName(), from.type()));
			}
		}

		public Resultat build() {
			return new Resultat(rows, columns, (System.nanoTime() - startTime) / 1_000_000);
		}

		public void addRow(Object[] data) {
			rows.add(new ResultatRow(data));
		}

	}

	public static ResultatBuilder create(List<? extends Column> columns) {
		return new ResultatBuilder(columns);
	}

	public Resultat(List<ResultatRow> rows, List<ResultatColumn> columns, long timeSpent) {
		this.rows = rows;
		this.columns = columns;
		this.timeSpent = timeSpent;
	}

	List<ResultatRow> rows = new ArrayList<>();
	List<ResultatColumn> columns = new ArrayList<>();

	public int size() {
		return rows.size();
	}

	public List<ResultatColumn> columns() {
		return columns;
	}

	public List<ResultatRow> values() {
		return rows;
	}

	public static class ResultatLine {
		ResultatRow row;
		Resultat from;
		
		public ResultatLine(ResultatRow row, Resultat from) {
			super();
			this.row = row;
			this.from = from;
		}

		public Object value(String columnName) {
			List<ResultatColumn> columns = from.columns;
			for (int i = 0; i < columns.size(); i++) {
				ResultatColumn column = columns.get(i);
				if (column.alias().equals(columnName)) {
					return row.values[i];
				}
			}
			throw new ColumnDoesntExist(columnName);
		}

		public Object value(int columnIndex) {
			List<ResultatColumn> columns = from.columns;
			return row.values[columnIndex-1];
		}

	}
	
	public ResultatLine rowAt(int i) {
		return new ResultatLine(rows.get(i), this);
	}


	@Override
	public String toString() {
		List<Integer> columnSize = new ArrayList<>();
		for (int i = 0; i < columns.size(); i ++) {
			columnSize.add(columns.get(i).displayName().length() + 2);
		}
		for (int i = 0; i < columns.size(); i ++) {
			final int i$ = i;
			OptionalInt max = rows.stream().map(r -> r.data()[i$]).mapToInt(s -> s.toString().length()).max();
			max.ifPresent(l -> {
				if (l > columnSize.get(i$)) {
					columnSize.set(i$, l);
				}
			});
		}
		
		
		
		StringBuilder res = new StringBuilder();
		
		res.append(rows.size() + " rows in " + timeSpent + "ms\n");
		
		StringBuilder lineFormat = new StringBuilder();
		StringBuilder lineHeaderFormat = new StringBuilder();
		Object[] headers = new Object[columns.size()];
		for (int i = 0; i < columns.size(); i ++) {
			if (i != 0) {
				lineFormat.append(" | ");
				lineHeaderFormat.append(" | ");
			}
			lineFormat.append("%"+columnSize.get(i)+"s");
			lineHeaderFormat.append(" %-"+(columnSize.get(i)-1)+"s");
			headers[i] = columns.get(i).displayName();
		}
		String lineFormat$ = lineFormat.toString();
		res.append(String.format(lineHeaderFormat.toString(), headers));
		res.append("\n");
		StringBuilder separator = new StringBuilder();
		for (int i = 0; i < columns.size(); i++) {
			if (i != 0) {
				separator.append("-+-");
			}
			for (int j = 0; j < columnSize.get(i); j++) {
				separator.append("-");
			}
		}
		res.append(separator);
		res.append("\n");
		
		for (int i = 0; i < rows.size(); i++) {
			Object[] row = new Object[columns.size()];
			for (int j = 0; j < columns.size(); j++) {
				row[j] = rows.get(i).data()[j];
			}
			res.append(String.format(lineFormat$, row));
			res.append("\n");
		}
		return res.toString();
	}


	public void aliases(List<String> colsAliases) {
		for (int i = 0; i < colsAliases.size(); i++) {
			String s = colsAliases.get(i);
			columns.get(i).setAlias(s);
		}
	}
	
}
