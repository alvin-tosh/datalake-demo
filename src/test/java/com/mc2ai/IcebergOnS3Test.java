package com.mc2ai;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

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

  String JDBC_CATALOG_USER = "admin";
  String JDBC_CATALOG_PASSWORD = "password";
  String CATALOG_NAME = "demo_local_s3";
  String SPARK_SQL_CATALOG = "spark.sql.catalog." + CATALOG_NAME;
  String DEMO_LOCAL_S3_NYC_LOGS_PATH = CATALOG_NAME + ".nyc.logs";

  String MINIO_ROOT_USER = "minioadmin";

  String MINIO_ROOT_PASSWORD = "minioadmin";

  String MINIO_DEFAULT_REGION = "us-east-1";

  String MINIO_WAREHOUSE_BUCKET = "warehouse";

  @TempDir Path tempPath;

  SparkSession spark;

  @Container
  private GenericContainer minioContainer =
      new GenericContainer("minio/minio:latest")
          .withEnv("MINIO_ROOT_USER", MINIO_ROOT_USER)
          .withEnv("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD)
          .withExposedPorts(9000, 9001)
          .withCommand("server /data --console-address :9001");

  @Container
  private PostgreSQLContainer postgresqlContainer =
      new PostgreSQLContainer(DockerImageName.parse("postgres:12.0"))
          .withDatabaseName(CATALOG_NAME)
          .withUsername(JDBC_CATALOG_USER)
          .withPassword(JDBC_CATALOG_PASSWORD);

  private void createBucket(String endpoint, String accessKey, String SecretKey, String bucket)
      throws Exception {
    MinioClient minioClient =
        MinioClient.builder().endpoint(endpoint).credentials(accessKey, SecretKey).build();

    boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
    if (!found) {
      minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
    } else {
      System.out.println("Bucket already exists:" + bucket);
    }
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    minioContainer.start();
    postgresqlContainer.start();

    String minioEndPoint =
        "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(9000);

    createBucket(minioEndPoint, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_WAREHOUSE_BUCKET);

    System.setProperty("aws.accessKeyId", MINIO_ROOT_USER);
    System.setProperty("aws.secretAccessKey", MINIO_ROOT_PASSWORD);
    System.setProperty("aws.region", MINIO_DEFAULT_REGION);

    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Java API Demo")
            .config(
                "spark.sql.extensions",
                org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions.class.getName())
            .config(SPARK_SQL_CATALOG, org.apache.iceberg.spark.SparkCatalog.class.getName())
            .config(
                SPARK_SQL_CATALOG + ".catalog-impl",
                org.apache.iceberg.jdbc.JdbcCatalog.class.getName())
            .config(
                SPARK_SQL_CATALOG + ".uri",
                "jdbc:postgresql://"
                    + postgresqlContainer.getHost()
                    + ":"
                    + postgresqlContainer.getMappedPort(5432)
                    + "/"
                    + CATALOG_NAME)
            .config(SPARK_SQL_CATALOG + ".jdbc.user", JDBC_CATALOG_USER)
            .config(SPARK_SQL_CATALOG + ".jdbc.password", JDBC_CATALOG_PASSWORD)
            .config(
                SPARK_SQL_CATALOG + ".io-impl", org.apache.iceberg.aws.s3.S3FileIO.class.getName())
            .config(SPARK_SQL_CATALOG + ".warehouse", "s3://" + MINIO_WAREHOUSE_BUCKET)
            .config(SPARK_SQL_CATALOG + ".s3.endpoint", minioEndPoint)
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
    postgresqlContainer.stop();
    minioContainer.stop();
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
