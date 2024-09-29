package eu.bosteels.duckdb.attach;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionManager;

import javax.sql.DataSource;
import java.sql.SQLException;

@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  public DataSource dataSource() throws SQLException {
    String url = "jdbc:duckdb:demo.duck.db";
    DuckDataSource dataSource = new DuckDataSource();
    dataSource.setUrl(url);
    dataSource.init();
    return dataSource;
  }

  @Bean
  public TransactionManager transactionManager(DataSource dataSource) {
    return new DataSourceTransactionManager(dataSource);
  }

}
