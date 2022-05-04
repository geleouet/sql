package fr.egaetan.sql.driver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class SLODriver implements Driver {

	static
	{
		// Register the JWDriver with DriverManager
		SLODriver driverInst = new SLODriver();
		try {
			DriverManager.registerDriver(driverInst);
		} catch (SQLException e) {
		}
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		return new SLOConnection();
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		System.out.println("Accept " + url);
		return true;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		return new DriverPropertyInfo[] {new DriverPropertyInfo("description", "SLOW")};
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 0;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}
}
