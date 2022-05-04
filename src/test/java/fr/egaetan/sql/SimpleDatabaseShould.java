package fr.egaetan.sql;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.query.Query;
import fr.egaetan.sql.result.Resultat;

public class SimpleDatabaseShould {
	
	@Test
	public void createTable_insert_select_entier() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.ENTIER)
				.build();
		table.insert(table.values().set("id", 1).set("value", 2));
		
		// WHEN
		Resultat res = new Query().select(table.column("value")) .from(table).execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(1);
		Assertions.assertThat(res.columns().size()).isEqualTo(1);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo(2);
	}
	
	@Test
	public void select_string() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		table.insert(table.values().set("id", 1).set("value", "John"));
		table.insert(table.values().set("id", 2).set("value", "Jack"));
		
		// WHEN
		Resultat res = new Query().select(table.column("value")) .from(table).execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(2);
		Assertions.assertThat(res.columns().size()).isEqualTo(1);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(1).value("value")).isEqualTo("Jack");
		
	}
	
	@Test
	// select value + 10 as plus10 from client;
	public void select_entier_formula() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.ENTIER)
				.build();
		table.insert(table.values().set("id", 1).set("value", 2));
		
		// WHEN
		Resultat res = new Query().select(Query.compound(table.column("value"), "plus10", (Integer v) -> v + 10))
				.from(table).execute();
		
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(1);
		Assertions.assertThat(res.columns().size()).isEqualTo(1);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("plus10");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.rowAt(0).value("plus10")).isEqualTo(12);
		
	}

	@Test
	public void select_string_formula() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		table.insert(table.values().set("id", 1).set("value", "hello"));
		
		// WHEN
		Resultat res = new Query().select(Query.compound(table.column("value"), "world", (String v) -> v + "world"))
				.from(table).execute();
		
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(1);
		Assertions.assertThat(res.columns().size()).isEqualTo(1);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("world");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("world")).isEqualTo("helloworld");
		
	}
	
	@Test
	public void select_string_multi_colonnes() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		table.insert(table.values().set("id", 1).set("value", "John"));
		table.insert(table.values().set("id", 2).set("value", "Jack"));
		
		// WHEN
		Resultat res = new Query().select(table.column("id"), table.column("value"))
				.from(table).execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(2);
		Assertions.assertThat(res.columns().size()).isEqualTo(2);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(1).value("value")).isEqualTo("Jack");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(1).value("id")).isEqualTo(2);
		
	}
	@Test
	public void select_string_filtered_colonnes_where() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		table.insert(table.values().set("id", 1).set("value", "John"));
		table.insert(table.values().set("id", 2).set("value", "Jack"));
		table.insert(table.values().set("id", 3).set("value", "Paul"));
		
		// WHEN
		Resultat res = new Query().select(table.column("value"))
				.from(table)
				.where(table.column("id")).isEqualTo(2)
				.execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(1);
		Assertions.assertThat(res.columns().size()).isEqualTo(1);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("Jack");
		
	}
	@Test
	public void select_where() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		table.insert(table.values().set("id", 1).set("value", "John"));
		table.insert(table.values().set("id", 2).set("value", "Jack"));
		
		// WHEN
		Resultat res = new Query().select(table.column("id"), table.column("value"))
				.from(table)
				.where(table.column("id")).isEqualTo(2)
				.execute();
		System.out.println(res);
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(1);
		Assertions.assertThat(res.columns().size()).isEqualTo(2);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("Jack");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(2);
		
	}
	@Test
	public void select_where_and() {
		// GIVEN
		Base base = Base.create();
		Table table = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("money", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		table.insert(table.values().set("id", 1).set("money", 0).set("value", "John"));
		table.insert(table.values().set("id", 2).set("money", 100).set("value", "Jack"));
		table.insert(table.values().set("id", 1).set("money", 100).set("value", "Roger"));
		table.insert(table.values().set("id", 1).set("money", 200).set("value", "Paul"));
		table.insert(table.values().set("id", 2).set("money", 100).set("value", "Rick"));
		table.insert(table.values().set("id", 2).set("money", 0).set("value", "Hugh"));
		
		// WHEN
		Resultat res = new Query().select(table.column("id"), table.column("value"))
				.from(table)
				.where(table.column("id")).isEqualTo(2)
				.and(table.column("money")).isEqualTo(0)
				.execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(1);
		Assertions.assertThat(res.columns().size()).isEqualTo(2);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("Hugh");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(2);
		
	}

	
}
