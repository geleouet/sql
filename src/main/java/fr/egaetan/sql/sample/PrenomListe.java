package fr.egaetan.sql.sample;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PrenomListe {
	
	private static class Prenom {
		String prenom;
		@SuppressWarnings("unused")
		String sex;
		String language;
		@SuppressWarnings("unused")
		String freq;

		public Prenom(String prenom, String sex, String language, String freq) {
			super();
			this.prenom = prenom.split(" ")[0];
			this.sex = sex;
			this.language = language;
			this.freq = freq;
		}

		private Prenom(String[] split) {
			this(split[0], split[1], split[2], split[3]);

		}

		public Prenom(String csvLine) {
			this(csvLine.split(";"));
		}

		public boolean isUsedIn(String string) {
			return language.contains(string);
		}

		@Override
		public String toString() {
			return prenom;
		}
	}

	public List<String> prenoms() {
		return firstnames(p -> p.isUsedIn("french"));
	}
	
	public List<String> firstnames() {
		return firstnames(__ -> true);
	}
	
	private List<String> firstnames(Predicate<? super Prenom> filter) {
		try {
			return Files.lines(Paths.get(this.getClass().getResource("/Prenoms.csv").toURI()), StandardCharsets.ISO_8859_1)
					.map(l -> new Prenom(l))
					.filter(filter)
					.map(p -> p.prenom)
					.collect(Collectors.toList());
		} catch (IOException | URISyntaxException e) {
			e.printStackTrace();
			return Collections.emptyList();
		}
	}
}
