package eu.bosteels.duckdb.attach;

import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"SqlDialectInspection", "SqlSourceToSinkFlow", "LoggingSimilarMessage"})
public class Db {

  private final DuckDBConnection connection;

  private static final Logger logger = LoggerFactory.getLogger(Db.class);

  public Db(String url) throws SQLException {
    this.connection = (DuckDBConnection) DriverManager.getConnection(url);
  }

  public Connection getConnection() throws SQLException {
    return connection.duplicate();
  }

  public interface Callback<T> {
    T doInTransaction(Statement statement) throws SQLException;
  }

  public interface CallbackNoResult {
    void inTransaction(Statement statement) throws SQLException;
  }

  public Long transactionId (Statement statement) throws SQLException {
    String query = "select txid_current() as txid";
    try (ResultSet rs = statement.executeQuery(query)) {
      if (rs.next()) {
        return rs.getLong(1);
      } else {
        return 0L;
      }
    }
  }


  public void doInTransaction(CallbackNoResult callback) throws SQLException {
    long txid_start = 0;
    try (Connection connection = getConnection();
         Statement statement = connection.createStatement()) {
      try {
        statement.execute("BEGIN TRANSACTION");
        txid_start = transactionId(statement);
        callback.inTransaction(statement);
      } catch (Exception e) {
        logger.error("transaction with id {} failed: {}", txid_start, e.getMessage());
        logger.atError().setCause(e).log("transaction failed");
        statement.execute("ROLLBACK");
        logger.warn("rollback done");
        throw e;
      } finally {
        statement.execute("COMMIT");
        statement.close();
      }
    }
  }


  public <T> T inTransaction(Callback<T> callback) throws SQLException {
    long txid_start = 0;
    boolean exception = false;
    try (Connection connection = getConnection();
         Statement statement = connection.createStatement()) {
      try {
        statement.execute("BEGIN TRANSACTION");
        txid_start = transactionId(statement);
        return callback.doInTransaction(statement);
      } catch (Exception e) {
        logger.atError()
              .setCause(e)
              .log("transaction failed: {}", e.getMessage());
        statement.execute("ROLLBACK");
        logger.warn("rollback done");
        exception = true;
        throw e;
      } finally {
        if (!exception) {
          Long txid_end = transactionId(statement);
          //noinspection StatementWithEmptyBody
          if (txid_start != txid_end) {
            // this happens when running loop2
            //logger.info("txid_start != txid_end ????");
          }
        }
        statement.execute("COMMIT");
        statement.close();
      }
    }
  }


  public List<Map<String,String>> runQuery(Statement statement, String query) throws SQLException {
    List<Map<String,String>> result = new ArrayList<>();
    try(ResultSet rs = statement.executeQuery(query)) {
      while (rs.next()) {
        Map<String, String> row = new HashMap<>();
        int colCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= colCount; i++) {
          String label = rs.getMetaData().getColumnLabel(i);
          String value = rs.getString(i);
          row.put(label, value);
        }
        result.add(row);
      }
    }
    return result;
  }

  public void close() throws SQLException {
    connection.close();
    logger.info("database closed");
  }

}
