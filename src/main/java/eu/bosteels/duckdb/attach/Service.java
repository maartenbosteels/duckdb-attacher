package eu.bosteels.duckdb.attach;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"SqlDialectInspection", "SqlSourceToSinkFlow"})
@Component
public class Service {

  private String databaseName = "db1";
  private int dbCounter = 0;

  private static final Logger logger = LoggerFactory.getLogger(Service.class);

  private final JdbcTemplate jdbcTemplate;

  @Autowired
  public Service(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
  }

  public void checkPoint() {
    jdbcTemplate.execute("checkpoint");
  }

  public Long transactionId () {
    String query = "select txid_current() as txid";
    return jdbcTemplate.queryForObject(query, Long.class);
  }

  @Transactional
  public void insertData(long id, String threadName) {
    long tx1 = transactionId();
    jdbcTemplate.execute("use " + databaseName);
    long tx2 = transactionId();
    logger.debug("tx1={} tx2={}", tx1, tx2);
    String data = RandomString.generate(5000);
    // we probably should use a prepared statement here ...
    String insert = "insert into table1(id, thread, data) values(%d, '%s', '%s')".formatted(id, threadName, data);
    jdbcTemplate.execute(insert);
  }

  @Transactional
  public void insertInMainDatabase(long id, String threadName) {
    String insert = "insert into main_database(id, thread, ts) select %d, '%s', current_timestamp".formatted(id, threadName);
    jdbcTemplate.execute(insert);
  }

  private void deleteDatabaseFile() {
    Path path = Path.of(".", databaseName);
    try {
      Files.deleteIfExists(path);
      logger.debug("Deleted file {}", path);
    } catch (IOException e) {
      logger.error("deleteDatabaseFile failed: {}", e.getMessage());
    }
  }

  @Transactional
  public void attachNew() {
    dbCounter++;
    String newDatabaseName = "db" + dbCounter + "_" + RandomString.generate(10) + "_db";
    logger.info("attachNew: old: {} new: {}", databaseName, newDatabaseName);
    databaseName = newDatabaseName;
    logger.info("attaching new database: {}", databaseName);
    String attach = "attach '%s' as %s".formatted(databaseName, databaseName);
    //String attach = "attach ':memory:' as %s".formatted(databaseName);
    // use :memory: is not really faster
    jdbcTemplate.execute(attach);
    jdbcTemplate.execute("use " + databaseName);
    jdbcTemplate.execute("create table %s.table1(id int, thread varchar, data varchar)".formatted(databaseName));
    long transactionId = transactionId();
    logger.info("attached new database {} in tx with id {}", databaseName, transactionId);
  }

  @Transactional
  public List<Map<String, Object>> query(String sql) {
    return jdbcTemplate.queryForList(sql);
  }

  @Transactional
  public void execute(String query) {
    jdbcTemplate.execute(query);
  }

  @Transactional
  public void exportAndAttachNewDatabase(boolean createNew) {
    logger.info("exportAndAttachNewDatabase: createNew={} databaseName={}", createNew, databaseName);
    jdbcTemplate.execute("use " + databaseName);
    String destinationDir = "./exports/" + databaseName + "/";
    String export = """
                    export database '%s'
                    (
                        FORMAT PARQUET,
                        COMPRESSION ZSTD,
                        ROW_GROUP_SIZE 100_000
                    )
                    """.formatted(destinationDir);
    jdbcTemplate.execute(export);
    jdbcTemplate.execute("ATTACH if not exists ':memory:' AS memory ");
    jdbcTemplate.execute("use memory ");
    jdbcTemplate.execute("detach %s".formatted(databaseName));
    deleteDatabaseFile();
    if (createNew) {
      attachNew();
    }
  }

}
