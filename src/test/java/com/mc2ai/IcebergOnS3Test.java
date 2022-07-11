package com.mc2ai;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

// TODO
public class IcebergOnS3Test {

  Schema schema =
      new Schema(
          Types.NestedField.required(1, "level", Types.StringType.get()),
          Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "message", Types.StringType.get()),
          Types.NestedField.optional(
              4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get())));

  PartitionSpec spec =
      PartitionSpec.builderFor(schema).hour("event_time").identity("level").build();

  String JDBC_CATALOG_URI = "jdbc:postgresql://192.168.31.65:5432/demo_catalog_local";
  String JDBC_CATALOG_USER = "admin";
  String JDBC_CATALOG_PASSWORD = "password";
  String CATALOG_NAME = "demo_local_s3";
  String SPARK_SQL_CATALOG = "spark.sql.catalog." + CATALOG_NAME;

  String DEMO_LOCAL_S3_NYC_LOGS_PATH = CATALOG_NAME + ".nyc.logs";

  @TempDir Path tempPath;

  SparkSession spark;

  @BeforeEach
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Java API Demo")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(SPARK_SQL_CATALOG, "org.apache.iceberg.spark.SparkCatalog")
            .config(SPARK_SQL_CATALOG + ".catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
            .config(SPARK_SQL_CATALOG + ".uri", JDBC_CATALOG_URI)
            .config(SPARK_SQL_CATALOG + ".jdbc.user", JDBC_CATALOG_USER)
            .config(SPARK_SQL_CATALOG + ".jdbc.password", JDBC_CATALOG_PASSWORD)
            .config(SPARK_SQL_CATALOG + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(SPARK_SQL_CATALOG + ".warehouse", "s3://warehouse")
            .config(SPARK_SQL_CATALOG + ".s3.endpoint", "http://192.168.31.65:9000")
            .config(SPARK_SQL_CATALOG + ".s3.delete-enabled", true)
            .config("spark.sql.defaultCatalog", CATALOG_NAME)
            .config("spark.eventLog.enabled", "true")
            //            .config("spark.eventLog.dir", tempPath + "/iceberg/spark-events")
            .config("spark.eventLog.dir", tempPath.toString())
            //            .config("spark.history.fs.logDirectory", tempPath +
            // "/iceberg/spark-events")
            .config("spark.history.fs.logDirectory", tempPath.toString())
            .getOrCreate();
    spark
        .sql(
            "CREATE TABLE "
                + DEMO_LOCAL_S3_NYC_LOGS_PATH
                + " (level string, event_time timestamp,message string,call_stack array<string>) USING iceberg"
                + " PARTITIONED BY (hours(event_time), identity(level))")
        .show();
  }

  @AfterEach
  void afterEach() {
    spark.sql("DROP table " + DEMO_LOCAL_S3_NYC_LOGS_PATH).show();
  }

  @Test
  public void sparkSqlS3Test() {
    writeS3DataBySparkSQL();
  }

  private void writeS3DataBySparkSQL() {

    spark.sparkContext().setLogLevel("ERROR");

    String query =
        "INSERT INTO "
            + DEMO_LOCAL_S3_NYC_LOGS_PATH
            + " VALUES "
            + "('info', timestamp 'today', 'Just letting you know!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
            + "('warning', timestamp 'today', 'You probably should not do this!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3')), "
            + "('error', timestamp 'today', 'This was a fatal application error!', array('stack trace line 1', 'stack trace line 2', 'stack trace line 3'))";

    spark.sql(query).show();

    String selectSQLStr = "SELECT * FROM " + DEMO_LOCAL_S3_NYC_LOGS_PATH;
    Dataset<Row> ds = spark.sql(selectSQLStr);
    ds.show();
    Assertions.assertEquals(3, ds.count());
  }
}
