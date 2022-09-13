package fr.egaetan.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class SLODriverTest {

	
	@Test
	public void test() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT id, prenom FROM client WHERE id = 10";

		
		// WHEN
		String result = "";
		try {
		   result = parseResult(con, requete, result);
		} catch (SQLException e) {
		   //traitement de l'exception
		}
		
		// THEN
		Assertions.assertThat(result).isEqualTo("10,aatu,");
	}

	private String parseResult(Connection con, String requete, String result) throws SQLException {
		Statement stmt = con.createStatement();
		   var results = stmt.executeQuery(requete);
		   ResultSetMetaData rsmd;
		   rsmd = results.getMetaData();
		   int nbCols = rsmd.getColumnCount();
		   System.out.println(nbCols);
		   while (results.next()) {
			      for (int i = 1; i <= nbCols; i++) {
			    	  System.out.print(results.getString(i) + " ");
			    	  result += results.getString(i) + ",";
			      }
			      result += "\n";
			      System.out.println();
			      
			   }
			   results.close();
		return result.strip();
	}

	@Test
	public void version() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT version()";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result).isEqualTo("LazySloDB 0.1,");
	}
	@Test
	public void shema_tables() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT * FROM information_schema.tables";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result.split("\\n").length).isEqualTo(3);
	}
	@Test
	public void SQLI_1() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT * FROM members WHERE username = 'achille'--' AND password = 'password' ";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result).contains("achille");
	}
	@Test
	public void SQLI_2() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT id, prenom FROM client WHERE prenom='achille' UNION ALL SELECT id, password FROM members WHERE id < 10--'";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result.split("\\n").length).isEqualTo(11);
	}
	@Test
	public void SQLI_3() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT username, password FROM members WHERE username='achille' and 1=0 UNION ALL SELECT 'admin', 'helloworld'";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result.split("\\n").length).isEqualTo(1);
	}
	
	@Test
	public void SQL_OR() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT username, password FROM members WHERE username='achille' OR username='alicia'";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result.split("\\n").length).isEqualTo(1);
	}
	
	@Test
	public void SQLI_4() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT username, password FROM members WHERE username='achille' AND (password='' OR 1=1)--'";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result.split("\\n").length).isEqualTo(1);
	}
	
	//@Test
	public void expErreur_tables() throws SQLException, ClassNotFoundException {
		// GIVEN
		Class.forName("fr.egaetan.sql.driver.SLODriver");
		Connection con = DriverManager.getConnection("jdbc:slo://db");
		String requete = "SELECT CASE WHEN (False) THEN CAST(1/0 AS INTEGER) ELSE NULL END";
		
		
		// WHEN
		String result = "";
		try {
			result = parseResult(con, requete, result);
		} catch (SQLException e) {
			//traitement de l'exception
			e.printStackTrace();
		}
		
		// THEN
		Assertions.assertThat(result.split("\\n").length).isEqualTo(2);
	}
	
	
}
