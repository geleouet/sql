package fr.egaetan.sql.common;

import fr.egaetan.sql.base.Table.Values;

public interface DataRow {

	Object[] data();

	void update(Values values);

}