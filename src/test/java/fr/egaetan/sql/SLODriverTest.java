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
			      System.out.println();
			      
			   }
			   results.close();
		} catch (SQLException e) {
		   //traitement de l'exception
		}
		
		// THEN
		Assertions.assertThat(result).isEqualTo("10,aatu,");
	}
}
