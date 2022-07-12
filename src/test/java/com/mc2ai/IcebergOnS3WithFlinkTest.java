package com.mc2ai;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;

import static com.mc2ai.TestHelper.*;

// TODO
public class IcebergOnS3WithFlinkTest {

  String CATALOG_NAME = "demo_local_s3";
  String SPARK_SQL_CATALOG = "spark.sql.catalog." + CATALOG_NAME;
  String DEMO_LOCAL_S3_NYC_LOGS_PATH = CATALOG_NAME + ".nyc.logs";

  @TempDir Path tempPath;

  SparkSession spark;

  @Container
  private GenericContainer minioContainer =
      minioContainer(MINIO_IMAGE, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD);

  @Container
  private PostgreSQLContainer postgresqlContainer =
      postgresqlContainer(POSTGRESQL_IMAGE, JDBC_CATALOG_USER, JDBC_CATALOG_PASSWORD, CATALOG_NAME);

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

    // TODO: Flink
  }

  @AfterEach
  void afterEach() {
    postgresqlContainer.stop();
    minioContainer.stop();
  }

  @Test
  public void writeTest() {}
}
