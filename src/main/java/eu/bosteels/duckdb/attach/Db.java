package eu.bosteels.duckdb.attach;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.*;
import java.util.*;

@SuppressWarnings({"SqlDialectInspection", "SqlSourceToSinkFlow", "LoggingSimilarMessage"})
public class Db {

  private final DuckDataSource dataSource;
  private final JdbcTemplate jdbcTemplate;

  private final JdbcTransactionManager transactionManager;

  private static final Logger logger = LoggerFactory.getLogger(Db.class);

  public Db(String url) throws SQLException {
    this.dataSource = new DuckDataSource();
    this.dataSource.setUrl(url);
    this.dataSource.init();
    logger.info("DuckDataSource created with url={}", url);
    jdbcTemplate = new JdbcTemplate(this.dataSource);
    transactionManager = new JdbcTransactionManager(this.dataSource);
    transactionManager.afterPropertiesSet();
  }

  public interface Callback<T> {
    T doInTransaction();
  }

  public interface CallbackNoResult {
    void inTransaction();
  }

  public Long transactionId () {
    String query = "select txid_current() as txid";
    return jdbcTemplate.queryForObject(query, Long.class);
  }


  public void doInTransaction(CallbackNoResult callback) throws SQLException {
    //long txid_start = 0;
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

    transactionTemplate.execute(_ -> {
        long txid_start = transactionId();
        callback.inTransaction();
        long txid_end = transactionId();
        if (txid_start != txid_end) {
          logger.debug("txid_start != txid_end, {} != {}", txid_start, txid_end);
        }
        return null;
    });
  }

  public List<Map<String, Object>> query(String sql) {
    return inTransaction(() -> runQuery(sql));
  }


  public <T> T inTransaction(Callback<T> callback) {
    TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
    return transactionTemplate.execute(_ -> {
      long txid_start = transactionId();
      try {
        return callback.doInTransaction();
      } finally {
        long txid_end = transactionId();
        if (txid_start != txid_end) {
          logger.warn("txid_start != txid_end, {} != {}", txid_start, txid_end);
        }
      }
    }
    );
  }

  public List<Map<String,Object>> runQuery(String query) {
    return jdbcTemplate.queryForList(query);
  }

  public void execute(String query) {
    jdbcTemplate.execute(query);
  }

  public void close() {
    dataSource.close();
    logger.info("dataSource closed");
  }

}
