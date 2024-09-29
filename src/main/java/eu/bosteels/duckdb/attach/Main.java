package eu.bosteels.duckdb.attach;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
@Component
public class Main {

  private final Service service;
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private final AtomicInteger insertCounter = new AtomicInteger(0);
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final int THREADS = 200;
  private static final int INSERTS_PER_THREAD = 2000;

  @Autowired
  public Main(Service service) throws SQLException {
    this.service = service;
  }

  @PostConstruct
  private void start() throws InterruptedException, IOException {
    logger.error("starting ...");
    File exportsDir = new File("./exports");
    deleteFolderRecursively(exportsDir.toPath());
    boolean dirCreated = exportsDir.mkdir();
    logger.info("exportsDir created = {}", dirCreated);

    service.execute("create or replace table main_database(id int, thread varchar, ts timestamp)");
    var tables = service.query("show all tables");
    logger.info("all_tables: {}", tables);

    service.attachNew();
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
      this.exportAndAttachNewDatabaseInTransaction(false);
    }
    var rowsExported = service.query(
                    "select sum(length(data)) as bytes, bytes/1000^3 as GBytes, count(1) as rows " +
                            "from 'exports/**/table_.parquet'");
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


  public void exportAndAttachNewDatabaseInTransaction(boolean createNew) {
    readWriteLock.writeLock().lock();
    try {
      logger.info("database_size = {}", service.query("CALL pragma_database_size()"));
      service.exportAndAttachNewDatabase(createNew);
      service.checkPoint();
      insertCounter.set(0);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }


  public void loop() throws SQLException {
    for (int i=0; i<INSERTS_PER_THREAD; i++) {
      int insertsDone = insertCounter.getAndIncrement();
      if (insertsDone == 1000) {
        this.exportAndAttachNewDatabaseInTransaction(true);
      }
      this.insert(i);
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
      service.insertInMainDatabase(id, threadName);
      service.insertData(id, threadName);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

}
