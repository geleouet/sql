package fr.egaetan.sql.base;

import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import fr.egaetan.sql.base.Table.ColumnType;
import fr.egaetan.sql.sample.PrenomListe;

public class DemoBase {

	public static Base base() {
		Base base = new Base();
		createTableClient(base);
		createTableMembers(base);
		return base;
	}

	private static String generateRandom(String chars, Random random) {
		int len = chars.length();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < len; i++) {
			int randomIndex = random.nextInt(chars.length());
			sb.append(chars.charAt(randomIndex));
		}

		return sb.toString();
	}
	

	public static Object generatePassword() {
		SecureRandom random = new SecureRandom();
		String from = generateRandom("0123456789", random)
				 + generateRandom("&~#%+-_", random)
				 + generateRandom("abcdefghijklmnopqrstuvwxyz", random)
				 + generateRandom("ABCDEFGHIJKLMNOPQRSTUVWXYZ", random)
				 ;
		
		List<Integer> indexes = IntStream.range(0, from.length()).mapToObj(i -> i).collect(Collectors.toList());
		java.util.Collections.shuffle(indexes, random);
		
		String p = "";
		for (int i = 0; i < 16; i++) {
			p += from.charAt(indexes.get(i));
		}		
		return p;
	}
	
	public static String generateCode() {
		SecureRandom random = new SecureRandom();
		String from = generateRandom("0123456789", random)
				 + generateRandom("abcdefghijklmnopqrstuvwxyz", random)
				 + generateRandom("ABCDEFGHIJKLMNOPQRSTUVWXYZ", random)
				 ;
		
		List<Integer> indexes = IntStream.range(0, from.length()).mapToObj(i -> i).collect(Collectors.toList());
		java.util.Collections.shuffle(indexes, random);
		
		String p = "";
		for (int i = 0; i < 8; i++) {
			p += from.charAt(indexes.get(i));
		}		
		return p;
	}

	
	private static Table createTableMembers(Base base) {
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

	
}
