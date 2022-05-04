package fr.egaetan.sql;

import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import fr.egaetan.sql.base.Base;
import fr.egaetan.sql.base.Table;
import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.query.Query;
import fr.egaetan.sql.result.Resultat;
import fr.egaetan.sql.sample.PrenomListe;

public class SampleDatabaseTest {

	@Test
	public void show_some_performance_issues() {
		// GIVEN
		Base base = Base.create();
		Table tableClient = createTableClient(base);
		Table tableColor = createTableColor(base);
		Table tableCity = createTableCity(base);
		
		
		// WHEN
		Resultat res = new Query().select(tableClient.column("prenom"), tableColor.column("color"), tableCity.column("name"))
				.from(tableClient)
				.innerJoin(tableColor).on(tableClient.column("color")).isEqualTo(tableColor.column("id"))
				.innerJoin(tableCity).on(tableClient.column("city")).isEqualTo(tableCity.column("id"))
				.execute();
		
		System.out.println(res);
		// THEN
		Assertions.assertThat(res.size()).isEqualTo(10336);
		Assertions.assertThat(res.columns().size()).isEqualTo(3);
	}

	private Table createTableColor(Base base) {
		Table tableColor = base
				.createTable("color")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("color", ColumnType.STRING)
				.build();
		tableColor.insert(tableColor.values().set("id", 1).set("color", "Blue"));
		tableColor.insert(tableColor.values().set("id", 2).set("color", "Red"));
		tableColor.insert(tableColor.values().set("id", 3).set("color", "White"));
		tableColor.insert(tableColor.values().set("id", 4).set("color", "Yellow"));
		tableColor.insert(tableColor.values().set("id", 5).set("color", "Green"));
		tableColor.insert(tableColor.values().set("id", 6).set("color", "Black"));
		tableColor.insert(tableColor.values().set("id", 7).set("color", "Grey"));
		tableColor.insert(tableColor.values().set("id", 8).set("color", "Pink"));
		return tableColor;
	}

	private Table createTableCity(Base base) {
		Table tableCity = base
				.createTable("city")
				.addColumn("id", ColumnType.ENTIER)
				.addColumn("name", ColumnType.STRING)
				.build();
		tableCity.insert(tableCity.values().set("id", 0).set("name", "Courbevoie"));
		tableCity.insert(tableCity.values().set("id", 1).set("name", "Paris"));
		tableCity.insert(tableCity.values().set("id", 2).set("name", "Brest"));
		tableCity.insert(tableCity.values().set("id", 3).set("name", "Rennes"));
		tableCity.insert(tableCity.values().set("id", 4).set("name", "Montpellier"));
		tableCity.insert(tableCity.values().set("id", 5).set("name", "Marseille"));
		tableCity.insert(tableCity.values().set("id", 6).set("name", "Lyon"));
		tableCity.insert(tableCity.values().set("id", 7).set("name", "Nice"));
		tableCity.insert(tableCity.values().set("id", 8).set("name", "Lilles"));
		tableCity.insert(tableCity.values().set("id", 9).set("name", "Toulouse"));
		return tableCity;
	}

	private Table createTableClient(Base base) {
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
}
