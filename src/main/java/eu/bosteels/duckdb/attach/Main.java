package eu.bosteels.duckdb.attach;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

@SuppressWarnings({"SqlDialectInspection", "DuplicatedCode"})
public class Main {

  private final Db db;

  private String databaseName = "db1";
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private final AtomicInteger insertCounter = new AtomicInteger(0);
  private int dbCounter = 0;
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final int THREADS = 100;
  private static final int INSERTS_PER_THREAD = 4000;

  public static void main(String[] args) throws SQLException, InterruptedException, IOException {
    logger.info("Main just started");
    Main main = new Main();
    main.start();
  }

  public Main() throws SQLException {
    String url = "jdbc:duckdb:demo.duck.db";
    this.db = new Db(url);
  }

  private void start() throws SQLException, InterruptedException, IOException {

    File exportsDir = new File("./exports");
    deleteFolderRecursively(exportsDir.toPath());
    boolean dirCreated = exportsDir.mkdir();
    logger.info("exportsDir created = {}", dirCreated);

    var data = db.inTransaction(() -> {
      db.execute("create or replace table main_database(id int, thread varchar, ts timestamp)");
      return db.runQuery("show all tables");
    });
    logger.info("all_tables: {}", data);
    db.doInTransaction(this::attachNew);
    try (ExecutorService executor = Executors.newFixedThreadPool(THREADS)) {
      for (int i = 0; i < THREADS; i++) {
        executor.submit(this::run);
      }
      executor.shutdown();
      logger.info("Started {} threads, now waiting until they're finished ...", THREADS);
      while (!executor.isTerminated()) {
        boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        logger.info("terminated = {}", terminated);
      }
      exportAndAttachNewDatabaseInTransaction(false);
    }
    var rowsExported = db.inTransaction(() ->
            db.runQuery(
                    "select sum(length(data)) as bytes, bytes/1000^3 as GBytes, count(1) as rows " +
                            "from 'exports/**/table_.parquet'"));
    db.close();
    logger.info("rowsExported = {}", rowsExported);
  }

  public static void deleteFolderRecursively(Path pathToBeDeleted) throws IOException {
    try (Stream<Path> paths = Files.walk(pathToBeDeleted)) {
      //noinspection ResultOfMethodCallIgnored
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
    logger.info("Path exists {}", Files.exists(pathToBeDeleted));
  }

  public void run() {
    String threadName = Thread.currentThread().getName();
    try {
      loop();
    } catch (Exception e) {
      logger.atError()
              .setCause(e)
              .setMessage("Exception in [{}] error: {}")
              .addArgument(threadName)
              .addArgument(e.getMessage())
              .log();

    }
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

  private void exportAndAttachNewDatabase(boolean createNew) {
    logger.info("exportAndAttachNewDatabase: createNew={} databaseName={}", createNew, databaseName);
    db.execute("use " + databaseName);
    String destinationDir = "./exports/" + databaseName + "/";
    String export = """
                    export database '%s'
                    (
                        FORMAT PARQUET,
                        COMPRESSION ZSTD,
                        ROW_GROUP_SIZE 100_000
                    )
                    """.formatted(destinationDir);
    db.execute(export);
    db.execute("ATTACH if not exists ':memory:' AS memory ");
    db.execute("use memory ");
    db.execute("detach %s".formatted(databaseName));
    deleteDatabaseFile();
    if (createNew) {
      attachNew();
    }
  }

  private void attachNew() {
    dbCounter++;
    String newDatabaseName = "db" + dbCounter + "_" + RandomString.generate(10) + "_db";
    logger.info("attachNew: old: {} new: {}", databaseName, newDatabaseName);
    databaseName = newDatabaseName;
    logger.info("attaching new database: {}", databaseName);
    String attach = "attach '%s' as %s".formatted(databaseName, databaseName);
    db.execute(attach);
    db.execute("use " + databaseName);
    db.execute("create table %s.table1(id int, thread varchar, data varchar)".formatted(databaseName));
    long transactionId = db.transactionId();
    logger.info("attached new database {} in tx with id {}", databaseName, transactionId);
  }

  private void exportAndAttachNewDatabaseInTransaction(boolean createNew) throws SQLException {
    readWriteLock.writeLock().lock();
    try {
      logger.info("database_size = {}", db.query("CALL pragma_database_size()"));
      db.doInTransaction(() -> exportAndAttachNewDatabase(createNew));
      db.doInTransaction(() -> db.execute("checkpoint"));
      insertCounter.set(0);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void insert(int id) throws SQLException {
    try {
      Thread.sleep(1);
    } catch (InterruptedException ignored) {
    }
    readWriteLock.readLock().lock();
    try {
      String threadName = Thread.currentThread().getName();
      db.doInTransaction(() -> {
        String insert = "insert into main_database(id, thread, ts) select %d, '%s', current_timestamp".formatted(id, threadName);
        db.execute(insert);
      });
      db.doInTransaction(() -> {
        db.execute("use " + databaseName);
        String data = RandomString.generate(5000);
        // we probably should use a prepared statement here ...
        String insert = "insert into table1(id, thread, data) values(%d, '%s', '%s')".formatted(id, threadName, data);
        db.execute(insert);
      });
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void loop() throws SQLException {
    for (int i=0; i<INSERTS_PER_THREAD; i++) {
      int insertsDone = insertCounter.getAndIncrement();
      if (insertsDone == 1000) {
        exportAndAttachNewDatabaseInTransaction(true);
      }
      insert(i);
    }
  }

}
