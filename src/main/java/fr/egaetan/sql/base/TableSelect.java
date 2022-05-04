package fr.egaetan.sql.base;

import java.util.List;
import java.util.stream.Stream;

import fr.egaetan.sql.common.Column;
import fr.egaetan.sql.common.DataRow;
import fr.egaetan.sql.exception.ColumnDoesntExist;

public interface TableSelect {

	Column column(String string);

	boolean has(Column column);

	String name();

	Stream<? extends DataRow> datas();
	
	List<? extends Column> columns();
	
	public default int indexOf(Column c) {
		for (int i = 0; i < columns().size(); i++) {
			if (columns().get(i) == c) {
				return i + 1;
			}
		}
		throw new ColumnDoesntExist("Colonne not present");
	}
	
}