package eu.bosteels.duckdb.attach;

import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.AbstractDriverBasedDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

public class DuckDataSource extends AbstractDriverBasedDataSource {

  private DuckDBConnection connection;
  private static final Logger logger = LoggerFactory.getLogger(DuckDataSource.class);

  public void init() throws SQLException {
    String url = getUrl();
    logger.info("Creating connection with url = {}", url);
    Objects.requireNonNull(url);
    this.connection = (DuckDBConnection) DriverManager.getConnection(url);
  }

  public void close() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        logger.info("Could not close DuckDBConnection = {}", e.getMessage());
      }
    }
  }


  @Override
  protected Connection getConnectionFromDriver(Properties props) throws SQLException {
    return connection.duplicate();
  }
}
