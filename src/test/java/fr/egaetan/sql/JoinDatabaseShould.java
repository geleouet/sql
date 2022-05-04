package fr.egaetan.sql;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.base.TableSelect;
import fr.egaetan.sql.exception.TableNameSpecifiedMoreThanOnce;
import fr.egaetan.sql.executor.QueryExecutor.Explain;
import fr.egaetan.sql.query.Query;
import fr.egaetan.sql.result.Resultat;

public class JoinDatabaseShould {

	@Test
	public void createTable_full_join() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		TableSelect tableColor = createTableColor(base);
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"))
				.from(tableClient, tableColor)
				.execute();
		
		// THEN
		
		Assertions.assertThat(res.size()).isEqualTo(10);
		Assertions.assertThat(res.columns().size()).isEqualTo(3);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
	}

	@Test
	public void createTable_inner_join_all_columns_requested() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableColor = base
				.createTable("color")
				.addColumn("id_color", ColumnType.ENTIER)
				.addColumn("color", ColumnType.STRING)
				.build();
		tableColor.insert(tableColor.values().set("id_color", 1).set("color", "Blue"));
		tableColor.insert(tableColor.values().set("id_color", 2).set("color", "Red"));
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"), tableColor.column("id_color"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id_color"))
				.execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(5);
		Assertions.assertThat(res.columns().size()).isEqualTo(4);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
		
		
		
	}
	
	@Test
	public void inner_join_all_columns_requested_same_name() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableColor = createTableColor(base);
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"), tableColor.column("id"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(5);
		Assertions.assertThat(res.columns().size()).isEqualTo(4);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
	}
	
	@Test
	public void rename_column_with_as() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableColor = createTableColor(base);
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), 
				tableColor.column("color"), tableColor.column("id").as("color_id"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(5);
		Assertions.assertThat(res.columns().size()).isEqualTo(4);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
	}
	
	@Test
	public void inner_join() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableColor = createTableColor(base);
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(5);
		Assertions.assertThat(res.columns().size()).isEqualTo(3);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("id");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("value");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.ENTIER);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
	}

	@Test
	public void inner_join_three_table() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableCity = createTableCity(base);
		Table tableColor = createTableColor(base);
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"), tableCity.column("name").as("city"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.innerJoin(tableCity).on(tableClient.column("id")).isEqualTo(tableCity.column("id"))
				.execute();
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(5);
		Assertions.assertThat(res.columns().size()).isEqualTo(4);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
		Assertions.assertThat(res.rowAt(0).value("city")).isEqualTo("London");
	}
	@Test
	public void inner_join_three_table_with_where() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableCity = createTableCity(base);
		Table tableColor = createTableColor(base);
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"), tableCity.column("name").as("city"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.innerJoin(tableCity).on(tableClient.column("id")).isEqualTo(tableCity.column("id"))
				.where(tableCity.column("id")).isEqualTo(1)
				.execute();
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(3);
		Assertions.assertThat(res.columns().size()).isEqualTo(4);
		Assertions.assertThat(res.rowAt(0).value("value")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(0).value("id")).isEqualTo(1);
		Assertions.assertThat(res.rowAt(0).value("color")).isEqualTo("Blue");
		Assertions.assertThat(res.rowAt(0).value("city")).isEqualTo("London");
	}
	@Test
	public void explain_select() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableCity = createTableCity(base);
		Table tableColor = createTableColor(base);
		
		// WHEN
		Explain res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"), tableCity.column("name").as("city"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.innerJoin(tableCity).on(tableClient.column("id")).isEqualTo(tableCity.column("id"))
				.where(tableCity.column("id")).isEqualTo(1)
				.explain()
				.execute();
		// THEN
		Assertions.assertThat(res.toString()).isNotEmpty();
	}

	@Test
	public void explain_analyse_select() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableCity = createTableCity(base);
		Table tableColor = createTableColor(base);
		
		// WHEN
		Explain res = new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"), tableCity.column("name").as("city"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
				.innerJoin(tableCity).on(tableClient.column("id")).isEqualTo(tableCity.column("id"))
				.where(tableCity.column("id")).isEqualTo(1)
				.explain()
				.analyse()
				.execute();
		// THEN
		Assertions.assertThat(res.toString()).isNotEmpty();
	}

	@Test
	public void not_specified_twice_the_same_table() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableColor = createTableColor(base);

		// THEN
		assertThrows(TableNameSpecifiedMoreThanOnce.class, () -> 

			// WHEN
			new Query().select(tableClient.column("id"), tableClient.column("value"), tableColor.column("color"))
			.from(tableClient, tableColor)
			.innerJoin(tableColor).on(tableClient.column("id")).isEqualTo(tableColor.column("id"))
			.execute()
		);
	}
	
	
	@Test
	//select relation.name, parent.name as parent_name
	//from relation inner join relation parent on relation.parent = parent.id;
	public void inner_join_on_same_table() {
		// GIVEN
		Base base = Base.create();
		Table tableRelation = createTableRelation(base);
		
		// WHEN
		TableSelect tableRelationAliasParent = tableRelation.alias("parent");
		Resultat res = new Query().select(tableRelation.column("name"), tableRelationAliasParent.column("name").as("parent_name"))
				.from(tableRelation)
				.innerJoin(tableRelationAliasParent).on(tableRelation.column("parent")).isEqualTo(tableRelationAliasParent.column("id"))
				.execute();
		
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(2);
		Assertions.assertThat(res.columns().size()).isEqualTo(2);
		Assertions.assertThat(res.columns().get(0).displayName()).isEqualTo("name");
		Assertions.assertThat(res.columns().get(1).displayName()).isEqualTo("parent_name");
		Assertions.assertThat(res.columns().get(0).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.columns().get(1).type()).isEqualTo(ColumnType.STRING);
		Assertions.assertThat(res.rowAt(0).value("name")).isEqualTo("Alice");
		Assertions.assertThat(res.rowAt(0).value("parent_name")).isEqualTo("Bob");
		Assertions.assertThat(res.rowAt(1).value("name")).isEqualTo("John");
		Assertions.assertThat(res.rowAt(1).value("parent_name")).isEqualTo("Alice");
	}
	
	private Table createTableRelation(Base base) {
		Table tableColor = base
				.createTable("relation")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("parent", ColumnType.ENTIER)
				.addColumn("name", ColumnType.STRING)
				.build();
		tableColor.insert(tableColor.values().set("id", 1).set("parent", 0).set("name", "Bob"));
		tableColor.insert(tableColor.values().set("id", 2).set("parent", 1).set("name", "Alice"));
		tableColor.insert(tableColor.values().set("id", 3).set("parent", 2).set("name", "John"));
		return tableColor;
	}
	
	private Table createTableColor(Base base) {
		Table tableColor = base
				.createTable("color")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("color", ColumnType.STRING)
				.build();
		tableColor.insert(tableColor.values().set("id", 1).set("color", "Blue"));
		tableColor.insert(tableColor.values().set("id", 2).set("color", "Red"));
		return tableColor;
	}

	private Table createTableCity(Base base) {
		Table tableColor = base
				.createTable("city")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("name", ColumnType.STRING)
				.build();
		tableColor.insert(tableColor.values().set("id", 1).set("name", "London"));
		tableColor.insert(tableColor.values().set("id", 2).set("name", "Paris"));
		return tableColor;
	}

	private Table createTableClient(Base base) {
		Table tableClient = base
				.createTable("client")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("value", ColumnType.STRING)
				.build();
		tableClient.insert(tableClient.values().set("id", 1).set("value", "John"));
		tableClient.insert(tableClient.values().set("id", 1).set("value", "Roger"));
		tableClient.insert(tableClient.values().set("id", 1).set("value", "Paul"));
		tableClient.insert(tableClient.values().set("id", 2).set("value", "Jack"));
		tableClient.insert(tableClient.values().set("id", 2).set("value", "Rick"));
		return tableClient;
	}
}
