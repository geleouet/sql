package fr.egaetan.sql.common;

import fr.egaetan.sql.base.Table.ColumnType;

public interface Column {

	public static class ColumnQualifiedName {
		String name;

		public ColumnQualifiedName(String name) {
			super();
			this.name = name;
		}

		public boolean identify(ColumnQualifiedName qualified) {
			return name.equalsIgnoreCase(qualified.name);
		}
		@Override
		public String toString() {
			return name;
		}
	}
	
	public default Column[] references() {
		return new Column[] {this};
	}
	
	
	public default Object read(Object[] datas) {
		return datas[0];
	}
	
	String displayName();

	default ColumnQualifiedName qualified() {
		return new ColumnQualifiedName(qualifiedName());
	}
	
	default String qualifiedName() {
		return displayName();
	}

	ColumnType type();

	default boolean need(Column c) {
		return c.qualified().identify(this.qualified());
	}

	default Column as(String displayName) {
		Column origin = this;
		return new Column() {

			@Override
			public String displayName() {
				return displayName;
			}
			
			@Override
			public String qualifiedName() {
				return origin.qualifiedName();
			}

			@Override
			public ColumnType type() {
				return origin.type();
			}
			
			
			
		};
	}

}